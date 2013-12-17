#import "IPOfflineQueue.h"
#import <dispatch/dispatch.h>
#import "FMDatabase.h"
#import "FMDatabaseAdditions.h"
#import "FMDatabasePool.h"
#import "FMDatabaseQueue.h"
#import "Reachability.h"
#import "DDLog.h"

// Log levels: off, error, warn, info, verbose
#if DEBUG
static const int ddLogLevel = LOG_LEVEL_INFO;
#else
static const int ddLogLevel = LOG_LEVEL_WARN;
#endif

static NSMutableSet *_activeQueues = nil;

#define TABLE_NAME @"queue2"
#define BUSY_RETRY_TIMEOUT 50

@interface IPOfflineQueue() {
    NSOperationQueue *_operationQueue;
    NSTimer *_autoResumeTimer;
    FMDatabaseQueue* _dbQueue;
    BOOL _stopped;
    BOOL _isNetworkReachable;
    
    NSNumber *_waitingForJob;
    NSDate *_waitingJobStartTime;
}
@end

@implementation IPOfflineQueue
@synthesize delegate;
@synthesize autoResumeInterval = _autoResumeInterval;
@synthesize name = _name;

#define DDLogCritical(frmt, ...)   LOG_OBJC_MAYBE(NO,   ddLogLevel, LOG_FLAG_ERROR,   0, frmt, ##__VA_ARGS__);[DDLog flushLog];

#pragma mark - Initialization and schema management

- (id)initWithName:(NSString *)name stopped:(BOOL)stopped delegate:(id<IPOfflineQueueDelegate>)d
{
    if ( (self = [super init]) ) {
        DDLogInfo(@"create queue %@ stopped: %d", name, stopped);
        @synchronized([self class]) {
            if (_activeQueues) {
                if ([_activeQueues containsObject:name]) {
                    [[NSException exceptionWithName:@"IPOfflineQueueDuplicateNameException"
                                             reason:[NSString stringWithFormat:@"[IPOfflineQueue] Queue already exists with name: %@", name] userInfo:nil
                      ] raise];
                }
                
                [_activeQueues addObject:name];
            } else {
                _activeQueues = [[NSMutableSet alloc] initWithObjects:name, nil];
            }
        }
        
        _dbQueue = [FMDatabaseQueue databaseQueueWithPath:[self dbFilePath]];
        
        self.autoResumeInterval = 0;
        self.delegate = d;
        
        _name = name;
        
        _operationQueue = [[NSOperationQueue alloc] init];
        _operationQueue.name = name;
        _operationQueue.maxConcurrentOperationCount = 1;
        
        [[NSNotificationCenter defaultCenter] addObserverForName:kReachabilityChangedNotification
                                                          object:nil
                                                           queue:[NSOperationQueue currentQueue]
                                                      usingBlock:^(NSNotification *aNotification) {
                                                          Reachability *reachability = aNotification.object;
                                                              
                                                          NetworkStatus remoteHostStatus = reachability.currentReachabilityStatus;
                                                      
                                                          _isNetworkReachable = remoteHostStatus != NotReachable;
                                                      
                                                          if (remoteHostStatus == NotReachable) {
                                                              DDLogInfo(@"suspend queue %@ via reachability", _name);
                                                              [self suspended:@"reachability"];
                                                          } else {
                                                              DDLogInfo(@"try resume queue %@ via reachability", _name);
                                                              [self tryAutoResume:@"Notwork reachable again"];
                                                          }
                                                      }];
        
        if (stopped) {
            [self stop:@"inital state is stopped"];
        } else {
            [self start:@"inital state is started"];
        }
        
        [self openDB];
    }
    return self;
}

-(void)close {
    DDLogInfo(@"queue dealloc: cleaning up");
    [self closeDB];
    _operationQueue = nil;
    
    @synchronized([self class]) { [_activeQueues removeObject:self.name]; }
    
    self.delegate = nil;
    _name = nil;
}

- (void)dealloc
{
    [self close];
}

-(NSString*)dbFilePath {
    return [[NSSearchPathForDirectoriesInDomains(NSCachesDirectory, NSUserDomainMask, YES) objectAtIndex:0] stringByAppendingPathComponent:
            [NSString stringWithFormat:@"%@.queue", _name]];
}

-(void)closeDB {
    [_dbQueue close];
}

-(FMDatabaseQueue *) dbQueue {
    return _dbQueue;
}

-(void)openDB {
    DDLogInfo(@"Is SQLite compiled with it's thread safe options turned on? %@!", [FMDatabase isSQLiteThreadSafe] ? @"Yes" : @"No");

    
    __block bool isNewQueue = YES;
    [self.dbQueue inDatabase:^(FMDatabase *db) {
        db.logsErrors = YES;
        
        FMResultSet *rs = [db executeQuery:[NSString stringWithFormat:@"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '%@'", TABLE_NAME]];
        
        int existingTables = [rs next] ? [rs intForColumnIndex:0] : 0;
        [rs close];
        
        if (existingTables < 1) {
            DDLogInfo(@"[IPOfflineQueue] Creating new schema");
            
            NSString *sql = [NSString stringWithFormat:@"CREATE TABLE %@ (taskid INTEGER PRIMARY KEY AUTOINCREMENT, params BLOB NOT NULL)", TABLE_NAME];
            NSError *error;
            BOOL created = [db update:sql withErrorAndBindings:&error];
            
            if (created == FALSE) {
                DDLogCritical(@"CRITICAL: Failed to create schema %@", error);
                
                NSDictionary *userInfo;
                
                if (error) {
                    userInfo = @{@"error": error};
                }
                
                [[NSException exceptionWithName:@"IPOfflineQueueDatabaseException"
                                         reason:@"Failed to create schema"
                                       userInfo:userInfo]
                 raise];
            }
            
            [self clear];
        } else {
            isNewQueue = NO;
        };
    }];
    
    if (!isNewQueue) {
        [self recoverPendingTasks];
    }
}

- (void)tryAutoResume:(NSString *)reason {
    if (!_isNetworkReachable) {
        // network is not reachable don't even bother
        return;
    }
    
    DDLogVerbose(@"tryAutoResume(%@): stopped: %d, waitingForJob: %@", _name, _stopped, _waitingForJob);
    
    if  (_waitingForJob != nil) {
        // when waiting for a job only finishJob can resume the queue
        return;
    }

    if  (_stopped) {
        // if the queue is manually stop we can't auto resume
        return;
    }
    
    [self resume:reason];
}

#pragma mark - Queue control

-(void)backgroundTaskBlock:(void (^)())block {
    __block UIBackgroundTaskIdentifier backgroundTask = [[UIApplication sharedApplication] beginBackgroundTaskWithExpirationHandler:^{
        backgroundTask = UIBackgroundTaskInvalid;
    }];
    
    @try {
        block();
    }
    @finally {
        [[UIApplication sharedApplication] endBackgroundTask:backgroundTask];
    }
}

-(void)enqueueOperation {
    DDLogVerbose(@"Adding operation to queue %@ suspended: %d, stopped: %d", _name, _isNetworkReachable, _stopped);
    [_operationQueue addOperationWithBlock:^{
        [self execute];
    }];
}

-(void)enqueueActionWithUserInfo:(NSDictionary *)userInfo
{
    [self.dbQueue inDatabase:^(FMDatabase *db) {
        [self backgroundTaskBlock:^{
            db.busyRetryTimeout = BUSY_RETRY_TIMEOUT;
            NSMutableData *data = [[NSMutableData alloc] init];
            NSKeyedArchiver *archiver = [[NSKeyedArchiver alloc] initForWritingWithMutableData:data];
            [archiver encodeObject:userInfo forKey:@"userInfo"];
            [archiver finishEncoding];
            
            NSError *error;
            NSString *sql = [NSString stringWithFormat:@"INSERT INTO %@ (params) VALUES (?)", TABLE_NAME];
            BOOL inserted = [db update:sql withErrorAndBindings:&error, data];
            
            if (inserted == FALSE) {
                DDLogCritical(@"CRITICAL: Failed to insert task table %@", error);
                
                NSDictionary *userInfo;
                
                if (error) {
                    userInfo = @{@"error": error};
                }
                
                [[NSException exceptionWithName:@"IPOfflineQueueDatabaseException"
                                         reason:[NSString stringWithFormat:@"Failed to insert new queued item"]
                                       userInfo:userInfo] raise];
            }
        }];
        
        [self enqueueOperation];
    }];
}

- (void)filterActionsUsingBlock:(IPOfflineQueueFilterBlock)filterBlock {
    // This is intentionally fuzzy and its deletions are not guaranteed (not protected from race conditions).
    // The idea is, for instance, for redundant requests not to be executed, such as "update list from server".
    // Obviously, multiple updates all in a row are redundant, but you also want to be able to queue them
    // periodically without worrying that a bunch are already in the queue.
    //
    // With this simple, quick-and-dirty method, you can e.g. delete any existing "update" requests before
    // adding a new one.
    
    [self.dbQueue inDatabase:^(FMDatabase *db) {
        FMResultSet *rs = [db executeQuery:[NSString stringWithFormat:@"SELECT taskid, params FROM %@ ORDER BY taskid", TABLE_NAME]];
        
        while ([rs next]) {
            sqlite_uint64 taskId = [rs unsignedLongLongIntForColumnIndex:0];
            NSData *blobData = [rs dataForColumnIndex:1];
            
            NSDictionary *userInfo = [self decodeTaskInfo:blobData];
            
            if (filterBlock(userInfo) == IPOfflineQueueFilterResultAttemptToDelete) {
                [self deleteTask:taskId db:db];
            }
        }
        
        [rs close];
    }];
}

- (void)clear {
    [self backgroundTaskBlock:^{
        [_operationQueue cancelAllOperations];
        [self.dbQueue inDatabase:^(FMDatabase *db) {
            db.busyRetryTimeout = BUSY_RETRY_TIMEOUT;
            NSString *sql = [NSString stringWithFormat:@"DELETE FROM %@", TABLE_NAME];
            NSError *error;
            BOOL deleted = [db update:sql withErrorAndBindings:&error];
            
            if (deleted == FALSE) {
                DDLogCritical(@"CRITICAL: Failed to delete all queued items %@", error);
                
                NSDictionary *userInfo;
                
                if (error) {
                    userInfo = @{@"error": error};
                }
                
                [[NSException exceptionWithName:@"IPOfflineQueueDatabaseException"
                                         reason:@"Failed to delete all queued items"
                                       userInfo:userInfo]
                 raise];
            }
        }];
    }];
}

- (void)waitForRetry {
    NSString *reason = [NSString stringWithFormat:@"Last task of %@ failed waiting for retry", _name];
    DDLogWarn(@"Last task of %@ failed waiting for retry", _name);
    
    [self suspended:reason];
    
    double delayInSeconds = 15.0;
    dispatch_time_t popTime = dispatch_time(DISPATCH_TIME_NOW, delayInSeconds * NSEC_PER_SEC);
    dispatch_queue_t queue = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0);
    dispatch_after(popTime, queue, ^{
        NSString *reason = [NSString stringWithFormat:@"Retry task %@ after %g seconds", _name, delayInSeconds];
        [self tryAutoResume:reason];
    });
}

- (void)waitForJob:(task_id)jobId {
    _waitingForJob = [NSNumber numberWithUnsignedLongLong:jobId];
    _waitingJobStartTime = [NSDate date];
    [self stop:[NSString stringWithFormat:@"Waiting for job id %lld", jobId]];
}

- (void)stop:(NSString *)reason {
    DDLogInfo(@"stop queue %@ because of %@", _name, reason);
    
    _stopped = TRUE;
    [self suspended:reason];
}

- (void)start:(NSString *)reason {
    DDLogInfo(@"start queue %@ becouse of %@", _name, reason);
    
    _stopped = FALSE;
    [self resume:reason];
}

- (void)suspended:(NSString *)reason {
    DDLogInfo(@"suspended queue %@ because of %@", _name, reason);
    
    if ([self.delegate respondsToSelector:@selector(offlineQueueWillSuspend:)]) {
        [self.delegate offlineQueueWillSuspend:self];
    }
    _operationQueue.suspended = YES;
}

- (void)resume:(NSString *)reason {
    DDLogInfo(@"resume queue %@ because of %@, %d tasks in queue", _name, reason, (int)_operationQueue.operationCount);
    
    if ([self.delegate respondsToSelector:@selector(offlineQueueWillResume:)]) {
        [self.delegate offlineQueueWillResume:self];
    }
    
    _operationQueue.suspended = NO;
}

- (void)items:(void (^)(NSDictionary *userInfo))callback {
    [self.dbQueue inDatabase:^(FMDatabase *db) {
        FMResultSet *rs = [db executeQuery:[NSString stringWithFormat:@"SELECT params FROM %@ ORDER BY taskid", TABLE_NAME]];
        
        while ([rs next]) {
            NSData *blobData = [rs dataForColumnIndex:0];
            NSDictionary *userInfo = [self decodeTaskInfo:blobData];
            
            callback(userInfo);
        }
        
        [rs close];
    }];
}

-(NSUInteger)count {
    return _operationQueue.operationCount;
}

-(void)recoverPendingTasks {
    [self.dbQueue inDatabase:^(FMDatabase *db) {
        FMResultSet *rs = [db executeQuery:[NSString stringWithFormat:@"SELECT COUNT(*) FROM %@", TABLE_NAME]];
        
        if (rs == nil) {
            NSDictionary *userInfo = @{@"code": [NSNumber numberWithInt:[db lastErrorCode]], @"message": [db lastErrorMessage]};
            
            NSError *error = [NSError errorWithDomain:@"sqlite"
                                                 code:[db lastErrorCode]
                                             userInfo:userInfo];
            
            DDLogCritical(@"CRITICAL: Failed to get amount of pending jobs %@", error);
            
            
            [[NSException exceptionWithName:@"IPOfflineQueueDatabaseException"
                                     reason:@"Failed to get amount of pending jobs"
                                   userInfo:nil] raise];
        }
        
        int pendingJobs = 0;
        BOOL hasData = [rs next];
        if (hasData) {
            pendingJobs = [rs intForColumnIndex:0];
        }
        
        [rs close];
        
        DDLogInfo(@"Queue %@ have %d pending jobs", _name, pendingJobs);
        
        if (pendingJobs > 0) {
            for (int i=0;i<pendingJobs;i++) {
                [self enqueueOperation];
            }
        }
    }];
}

// this task has failed, need to be rerun
// next time the queue will run again it will pop the task again
-(void)taskFailed:(task_id)taskId error:(NSError *)error{
    DDLogWarn(@"Task %lld failed", taskId);
    
    [self resetWaitingTask:taskId error:error]; // TODO run in the proper queue
}

-(void)finishTask:(task_id)taskId {
    DDLogInfo(@"Task %llu finished", taskId);
    
    [self.dbQueue inDatabase:^(FMDatabase *db) {
        [self deleteTask:taskId db:db];
        [self resume:@"resume after async task finished"];
    }];
}

-(void)resetWaitingTask:(task_id)taskId error:(NSError *)error {
    if ([_waitingForJob unsignedLongLongValue] == taskId) {
        if (error) {
            DDLogInfo(@"Task %llu finished with error %@ total time %f seconds", taskId, error, -[_waitingJobStartTime timeIntervalSinceNow]);
        } else {
            DDLogInfo(@"Task %llu finished total time %f seconds", taskId, -[_waitingJobStartTime timeIntervalSinceNow]);
        }
        _waitingForJob = nil;
        _waitingJobStartTime = nil;
    }
}


-(void)deleteTask:(task_id)taskId db:(FMDatabase *)db {
    [self backgroundTaskBlock:^{
        db.busyRetryTimeout = BUSY_RETRY_TIMEOUT;
        NSString *sql = [NSString stringWithFormat:@"DELETE FROM %@ WHERE taskid = ?", TABLE_NAME];
        NSError *error;
        BOOL deleted = [db update:sql withErrorAndBindings:&error, [NSNumber numberWithUnsignedLongLong:taskId]];
        
        if (deleted == FALSE) {
            DDLogCritical(@"CRITICAL: Failed to delete queued item after execution %@", error);
            
            NSDictionary *userInfo;
            
            if (error) {
                userInfo = @{@"error": error};
            }
            
            [[NSException exceptionWithName:@"IPOfflineQueueDatabaseException"
                                     reason:@"Failed to delete queued item after execution"
                                   userInfo:userInfo]
             raise];
        }
        
        [self resetWaitingTask:taskId error:nil];
    }];
}

- (void)execute {
    [self.dbQueue inDatabase:^(FMDatabase *db) {
        
        sqlite_uint64 taskId = 0;
        NSData *blobData;
        
        NSString *sql = [NSString stringWithFormat:@"SELECT taskid, params FROM %@ ORDER BY taskid LIMIT 1", TABLE_NAME];
        FMResultSet *rs = [db executeQuery:sql];
        
        if (rs == nil) {
            NSDictionary *userInfo = @{@"code": [NSNumber numberWithInt:[db lastErrorCode]], @"message": [db lastErrorMessage]};
            
            NSError *error = [NSError errorWithDomain:@"sqlite"
                                                 code:[db lastErrorCode]
                                             userInfo:userInfo];
            
            DDLogCritical(@"CRITICAL: Failed to select next queued item %@", error);
            
            [[NSException exceptionWithName:@"IPOfflineQueueDatabaseException"
                                     reason:@"Failed to select next queued item"
                                   userInfo:userInfo] raise];
        }
        
        BOOL hasData = [rs next];
        if (hasData) {
            taskId = [rs unsignedLongLongIntForColumnIndex:0];
            blobData = [rs dataForColumnIndex:1];
        }
        
        [rs close];
        
        BOOL isEmpty = hasData == FALSE;
        
        if (isEmpty) {
            return;
        }
        
        NSDictionary *userInfo = [self decodeTaskInfo:blobData];
        
        [_operationQueue addOperationWithBlock:^{
            IPOfflineQueueResult result = [self.delegate offlineQueue:self taskId:taskId executeActionWithUserInfo:userInfo];
            if (result == IPOfflineQueueResultSuccess) {
                [self.dbQueue inDatabase:^(FMDatabase *db) {
                    [self deleteTask:taskId db:db];
                }];
            } else if (result == IPOfflineQueueResultAsync) {
                [self waitForJob:taskId]; // Stop queue wait for the user to tell us that the task has finish (or failed)
            } else if (result == IPOfflineQueueResultFailureShouldRetry) {
                [self waitForRetry]; // Stop the queue, wait for retry
            }
        }];
    } async:YES];
}

- (NSDictionary *)decodeTaskInfo:(NSData *)blobData {
    NSKeyedUnarchiver *unarchiver = [[NSKeyedUnarchiver alloc] initForReadingWithData:blobData];
    NSDictionary *userInfo = [unarchiver decodeObjectForKey:@"userInfo"];
    [unarchiver finishDecoding];
#pragma clang diagnostic push
#pragma ide diagnostic ignored "UnusedValue"
    unarchiver = nil;
#pragma clang diagnostic pop
    
    return userInfo;
}

@end
