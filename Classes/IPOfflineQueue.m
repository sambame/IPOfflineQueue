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

#define kMaxRetrySeconds 10000

static NSMutableSet *_activeQueues = nil;

#define TABLE_NAME @"queue2"

@interface IPOfflineQueue() {
    NSOperationQueue *_operationQueue;
    NSTimer *_autoResumeTimer;
    
    BOOL _stopped;
    
    NSNumber *_waitingForJob;
    NSDate *_waitingJobStartTime;
    
    BOOL _waitingForRetry;
}
@end

@implementation IPOfflineQueue
@synthesize delegate;
@synthesize autoResumeInterval = _autoResumeInterval;
@synthesize name = _name;


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
        
        self.autoResumeInterval = 0;
        self.delegate = d;
        
        _name = name;

        [[NSNotificationCenter defaultCenter] addObserverForName:kReachabilityChangedNotification
                                                          object:nil
                                                           queue:nil
                                                      usingBlock:^(NSNotification *aNotification) {
                                                          dispatch_async(dispatch_get_main_queue(), ^{
                                                              Reachability *reachability = aNotification.object;
                                                              
                                                              NetworkStatus remoteHostStatus = reachability.currentReachabilityStatus;
                                                              
                                                              if (remoteHostStatus == NotReachable) {
                                                                  DDLogInfo(@"suspend queue %@ via reachability", _name);
                                                                  [self suspended:@"reachability"];
                                                              } else {
                                                                  DDLogInfo(@"try resume queue %@ via reachability", _name);
                                                                  [self tryAutoResume];
                                                              }
                                                          });
                                                      }];

        _operationQueue = [[NSOperationQueue alloc] init];
        _operationQueue.name = name;
        _operationQueue.maxConcurrentOperationCount = 1;
        
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

-(void)dropDB {
    [self closeDB];
    [[NSFileManager defaultManager] removeItemAtPath:[self dbFilePath] error:nil];
    [self openDB];
}

-(void)closeDB {
    FMDatabaseQueue *dbQueue = self.currentDbQueue;
    
    if (dbQueue) {
        [dbQueue close];
    }
}

-(NSString *) tlsEntry {
    return [NSString stringWithFormat:@"dbQueue%@", self.name];
}

-(FMDatabaseQueue *) currentDbQueue {
    NSMutableDictionary *tls = [NSThread currentThread].threadDictionary;
    
    return tls[self.tlsEntry];
}

-(FMDatabaseQueue *) dbQueue {
    FMDatabaseQueue *dbQueue = self.currentDbQueue;
    
    if (dbQueue == nil) {
        dbQueue = [FMDatabaseQueue databaseQueueWithPath:[self dbFilePath]];
        
        if (dbQueue == nil) {
            [[NSException exceptionWithName:@"IPOfflineQueueDatabaseException"
                                     reason:[NSString stringWithFormat:@"Failed to open database"] userInfo:nil
              ] raise];
        }

        NSMutableDictionary *tls = [[NSThread currentThread] threadDictionary];
        tls[self.tlsEntry] = dbQueue;
    }
    
    return dbQueue;
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
            BOOL deleted = [db update:sql withErrorAndBindings:&error];
            
            if (deleted == FALSE) {
                [[NSException exceptionWithName:@"IPOfflineQueueDatabaseException"
                                         reason:@"Failed to create schema"
                                       userInfo:@{@"error": error}]
                 raise];
            }
            
            [self clear:db];
        } else {
            isNewQueue = NO;
        };
    }];
    
    if (!isNewQueue) {
        [self recoverPendingTasks];
    }
}

- (void)tryAutoResume {
    if (_operationQueue.isSuspended == FALSE) {
        return;
    }
    
    DDLogVerbose(@"tryAutoResume(%@): stopped: %d, waitingForJob: %@", _name, _stopped, _waitingForJob);
    
    if (!_stopped && _waitingForJob == nil) {
        BOOL canAutoResume = TRUE;
        if ([self.delegate respondsToSelector:@selector(offlineQueueShouldAutomaticallyResume:)]) {
            canAutoResume = [self.delegate offlineQueueShouldAutomaticallyResume:self];
        }
        
        DDLogVerbose(@"canAutoResume(%@): %d", _name, canAutoResume);
        
        if (canAutoResume) {
            [self resume:@"auto resume"];
        }
    }    
}

- (void)autoResumeTimerFired:(NSTimer*)timer {
    [self tryAutoResume];
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
    DDLogVerbose(@"Adding operation to queue %@ suspended: %d", _name, _operationQueue.isSuspended);
    [_operationQueue addOperationWithBlock:^{
        [self execute];
    }];
}

- (void)enqueueActionWithUserInfo:(NSDictionary *)userInfo
{
    [self.dbQueue inDatabase:^(FMDatabase *db) {
        [self backgroundTaskBlock:^{
            NSMutableData *data = [[NSMutableData alloc] init];
            NSKeyedArchiver *archiver = [[NSKeyedArchiver alloc] initForWritingWithMutableData:data];
            [archiver encodeObject:userInfo forKey:@"userInfo"];
            [archiver finishEncoding];
            archiver = nil;

            NSError *error;
            NSString *sql = [NSString stringWithFormat:@"INSERT INTO %@ (params) VALUES (?)", TABLE_NAME];
            BOOL inserted = [db update:sql withErrorAndBindings:&error, data];
            
            if (inserted == FALSE) {
                [[NSException exceptionWithName:@"IPOfflineQueueDatabaseException"
                                         reason:[NSString stringWithFormat:@"Failed to insert new queued item"]
                                       userInfo:@{@"error": error}
                  ] raise];
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
            sqlite_uint64 taskId = [rs intForColumnIndex:0];
            NSData *blobData = [rs dataForColumnIndex:1];
            
            NSKeyedUnarchiver *unarchiver = [[NSKeyedUnarchiver alloc] initForReadingWithData:blobData];
            NSDictionary *userInfo = [unarchiver decodeObjectForKey:@"userInfo"];
            [unarchiver finishDecoding];
            unarchiver = nil;
            
            if (filterBlock(userInfo) == IPOfflineQueueFilterResultAttemptToDelete) {
                [self deleteTask:taskId db:db];
            }
        }
        
        [rs close];
    }];
}

- (void)clear:(FMDatabase*)db {
    [self backgroundTaskBlock:^{
        [_operationQueue cancelAllOperations];
        [self.dbQueue inDatabase:^(FMDatabase *db) {
            NSString *sql = [NSString stringWithFormat:@"DELETE FROM %@", TABLE_NAME];
            NSError *error;
            BOOL deleted = [db update:sql withErrorAndBindings:&error];
            
            if (deleted == FALSE) {
                [[NSException exceptionWithName:@"IPOfflineQueueDatabaseException"
                                         reason:@"Failed to delete all queued items"
                                       userInfo:@{@"error": error}]
                 raise];
            }
        }];
    }];
}

- (void)waitForRetry {
    NSString *reason = [NSString stringWithFormat:@"Last task of %@ failed waiting for retry", _name];
    DDLogError(@"Last task of %@ failed waiting for retry", _name);
    
    _waitingForRetry = TRUE;
    [self suspended:reason];
}

- (void)waitForJob:(int)jobId {
    _waitingForJob = [NSNumber numberWithInt:jobId];
    _waitingJobStartTime = [NSDate date];
    [self stop:[NSString stringWithFormat:@"Waiting for job id %d", jobId]];
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
    DDLogInfo(@"resume queue %@ because of %@, %d tasks in queue", _name, reason, _operationQueue.operationCount);
    
    if ([self.delegate respondsToSelector:@selector(offlineQueueWillResume:)]) {
        [self.delegate offlineQueueWillResume:self];
    }
    
    _operationQueue.suspended = NO;
}

- (NSTimeInterval)autoResumeInterval {
    return _autoResumeInterval;
}

- (void)setAutoResumeInterval:(NSTimeInterval)newInterval
{
    if (_autoResumeInterval == newInterval) {
        return;
    }
    
    _autoResumeInterval = newInterval;
    
    // Ensure that this always runs on the main thread for simple timer scheduling
    dispatch_async(dispatch_get_main_queue(), ^{
        @synchronized(self) {
            if (_autoResumeTimer) {
                [_autoResumeTimer invalidate];
                _autoResumeTimer = nil;
            }

            if (newInterval > 0) {
                _autoResumeTimer = [NSTimer scheduledTimerWithTimeInterval:newInterval target:self selector:@selector(autoResumeTimerFired:) userInfo:nil repeats:YES];
            } else {
                _autoResumeTimer = nil;
            }
        }
    });
}

- (void)items:(void (^)(NSDictionary *userInfo))callback {
    [self.dbQueue inDatabase:^(FMDatabase *db) {
        FMResultSet *rs = [db executeQuery:[NSString stringWithFormat:@"SELECT params FROM %@ ORDER BY taskid", TABLE_NAME]];

        while ([rs next]) {
            NSData *blobData = [rs dataForColumnIndex:0];
            
            NSKeyedUnarchiver *unarchiver = [[NSKeyedUnarchiver alloc] initForReadingWithData:blobData];
            NSDictionary *userInfo = [unarchiver decodeObjectForKey:@"userInfo"];
            [unarchiver finishDecoding];
            unarchiver = nil;
            
            callback(userInfo);
        }
        
        [rs close];
    }];
}

-(void)recoverPendingTasks {
    [self.dbQueue inDatabase:^(FMDatabase *db) {
        FMResultSet *rs = [db executeQuery:[NSString stringWithFormat:@"SELECT COUNT(*) FROM %@", TABLE_NAME]];

        if (rs == nil) {
            // Some other error
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
-(void)taskFailed:(int)taskId error:(NSError *)error{
    DDLogError(@"Task %d failed", taskId);
    
    [self resetWaitingTask:taskId error:error]; // TODO run in the proper queue
}

-(void)finishTask:(int)taskId {
    DDLogInfo(@"Task %d finished", taskId);
    
    [self.dbQueue inDatabase:^(FMDatabase *db) {
        [self deleteTask:taskId db:db];
        [self resume:@"resume after async task finished"];
    }];
}

-(void)resetWaitingTask:(int)taskId error:(NSError *)error {
    if ([_waitingForJob integerValue] == taskId) {
        if (error) {
            DDLogInfo(@"Task %d finished with error %@ total time %f seconds", taskId, error, [_waitingJobStartTime timeIntervalSinceNow]);
        } else {
            DDLogInfo(@"Task %d finished total time %f seconds", taskId, [_waitingJobStartTime timeIntervalSinceNow]);
        }
        _waitingForJob = nil;
        _waitingJobStartTime = nil;
    }
}


-(void)deleteTask:(int)taskId db:(FMDatabase *)db {
    [self backgroundTaskBlock:^{
        NSString *sql = [NSString stringWithFormat:@"DELETE FROM %@ WHERE taskid = ?", TABLE_NAME];
        NSError *error;
        BOOL deleted = [db update:sql withErrorAndBindings:&error, [NSNumber numberWithInt:taskId]];
        
        if (deleted == FALSE) {
            [[NSException exceptionWithName:@"IPOfflineQueueDatabaseException"
                                     reason:@"Failed to delete queued item after execution"
                                   userInfo:@{@"error": error}]
             raise];
        }
        
        [self resetWaitingTask:taskId error:nil];
    }];
}

- (void)execute {
    [self.dbQueue inDatabase:^(FMDatabase *db) {
    
        sqlite_uint64 taskId;
        NSData *blobData;
        
        NSString *sql = [NSString stringWithFormat:@"SELECT taskid, params FROM %@ ORDER BY taskid LIMIT 1", TABLE_NAME];
        FMResultSet *rs = [db executeQuery:sql];
        
        if (rs == nil) {
            // Some other error
            [[NSException exceptionWithName:@"IPOfflineQueueDatabaseException"
                                     reason:@"Failed to select next queued item"
                                   userInfo:nil] raise];
        }
        
        BOOL hasData = [rs next];
        if (hasData) {
            taskId = [rs intForColumnIndex:0];
            blobData = [rs dataForColumnIndex:1];
        }
        
        [rs close];
        
        BOOL isEmpty = hasData == FALSE;
        
        if (isEmpty) {
            return;
        }
        
        NSKeyedUnarchiver *unarchiver = [[NSKeyedUnarchiver alloc] initForReadingWithData:blobData];
        NSDictionary *userInfo = [unarchiver decodeObjectForKey:@"userInfo"];
        [unarchiver finishDecoding];
        unarchiver = nil;
        
        IPOfflineQueueResult result = [self.delegate offlineQueue:self taskId:taskId executeActionWithUserInfo:userInfo];
        if (result == IPOfflineQueueResultSuccess) {
            [self deleteTask:taskId db:db];
        } else if (result == IPOfflineQueueResultAsync) {
            [self waitForJob:taskId]; // Stop queue wait for the user to tell us that the task has finish (or failed)
        } else if (result == IPOfflineQueueResultFailureShouldRetry) {
            [self waitForRetry]; // Stop the queue, wait for retry
        }
    }];
}

@end
