#import <Foundation/Foundation.h>


typedef enum {
   IPOfflineQueueResultSuccess = 0,
   IPOfflineQueueResultFailureShouldRetry,
   IPOfflineQueueResultAsync
} IPOfflineQueueResult;

typedef enum {
    IPOfflineQueueFilterResultAttemptToDelete = 0,
    IPOfflineQueueFilterResultNoChange
} IPOfflineQueueFilterResult;

typedef IPOfflineQueueFilterResult (^IPOfflineQueueFilterBlock)(NSDictionary *userInfo);

typedef unsigned long long int task_id;

@class IPOfflineQueue;


@protocol IPOfflineQueueDelegate <NSObject>

// This method will always be called on a thread that's NOT the main thread. So don't call UIKit from it.
// Feel free to block the thread until synchronous NSURLConnections, etc. are completed.
//
// Returning IPOfflineQueueResultSuccess will delete the task.
// Returning IPOfflineQueueResultFailureShouldPauseQueue will pause the queue and the same task will be retried when the queue is resumed.
//  Typically, you'd only return this if the internet connection is offline or some other global condition prevents ALL queued tasks from executing.
@required
- (IPOfflineQueueResult)offlineQueue:(IPOfflineQueue *)queue taskId:(task_id)taskId executeActionWithUserInfo:(NSDictionary *)userInfo;

@optional
// Called before auto-resuming upon Reachability changes, app reactivation, or autoResumeInterval elapsed
- (BOOL)offlineQueueShouldAutomaticallyResume:(IPOfflineQueue *)queue;

-(void)offlineQueueWillResume:(IPOfflineQueue *)queue;
-(void)offlineQueueWillSuspend:(IPOfflineQueue *)queue;
@end

@interface IPOfflineQueue : NSObject

// name must be unique among all current queue instances, and must be valid as part of a filename, e.g. "downloads" or "main"
- (id)initWithName:(NSString *)name stopped:(BOOL)stopped delegate:(id<IPOfflineQueueDelegate>)delegate;

// userInfo must be serializable
- (void)enqueueActionWithUserInfo:(NSDictionary *)userInfo;

- (NSUInteger)count;

- (void)close;
- (void)clear;

- (void)stop:(NSString *)reason;
- (void)start:(NSString *)reason;
- (void)finishTask:(task_id)taskId;
- (void)taskFailed:(task_id)taskId error:(NSError *)error;

- (void)items:(void (^)(NSDictionary *userInfo))callback;

// This is intentionally fuzzy and its deletions are not guaranteed (not protected from race conditions).
// The idea is, for instance, for redundant requests not to be executed, such as "get list from server".
// Obviously, multiple updates all in a row are redundant, but you also want to be able to queue them
// periodically without worrying that a bunch are already in the queue.
//
// With this simple, quick-and-dirty method, you can e.g. delete any existing "get list" requests before
// adding a new one.
//
// But since it does not guarantee that any filtered-out commands won't execute (or finish current executions),
// it should only be used to remove requests that won't have negative side effects if they're still performed,
// such as read-only requests.
//
- (void)filterActionsUsingBlock:(IPOfflineQueueFilterBlock)filterBlock;


@property (nonatomic, weak) id<IPOfflineQueueDelegate> delegate;
@property (nonatomic, readonly) NSString *name;
@property (nonatomic) NSTimeInterval autoResumeInterval;

@end
