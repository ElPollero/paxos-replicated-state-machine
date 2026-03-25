package didameetings.server;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.CompletableFuture;

/**
 * Fast Paxos for TOPIC commands with logical timestamp synchronization:
 * Phase 0: Wait for logical timestamp synchronization
 * Phase 1: Execute the command directly (dependencies guaranteed resolved)
 * 
 * Uses a sequential queue to prevent race conditions between concurrent Fast Paxos requests
 */
public class fastTopicManager {
    
    private final DidaMeetingsServerState serverState;
    
    // Fast Paxos queue for sequential processing
    private final BlockingQueue<fastTopicQueueItem> fastTopicQueue = new LinkedBlockingQueue<>();
    private final Thread fastTopicWorker;
    
    // Track active request waiting for timestamp synchronization (for predictive execution)
    private volatile fastTopicQueueItem activeWaitingRequest = null;
    
    /** Queue item for Fast Paxos requests */
    private static class fastTopicQueueItem {
        final RequestRecord requestRecord;
        final int clientTimestamp;
        final CompletableFuture<Boolean> future;
        volatile boolean predictiveResponseSent = false; // Track if we already sent predictive response
        
        fastTopicQueueItem(RequestRecord requestRecord, int clientTimestamp, CompletableFuture<Boolean> future) {
            this.requestRecord = requestRecord;
            this.clientTimestamp = clientTimestamp;
            this.future = future;
        }
    }
    
    public fastTopicManager(DidaMeetingsServerState serverState) {
        this.serverState = serverState;
        
        // Start the Fast Paxos worker thread
        this.fastTopicWorker = new Thread(() -> processfastTopicQueue(), "fastTopic-Worker-" + serverState.my_id);
        this.fastTopicWorker.setDaemon(true);
        this.fastTopicWorker.start();
        
        System.out.println("[FAST_TOPIC] Started Fast Paxos worker thread for server " + serverState.my_id);
    }

    /**
     * 1. Try immediate execution (if meeting+participant exist)
     * 2. If can't execute and current_timestamp >= client_timestamp: return NO
     * 3. If current_timestamp < client_timestamp: queue and wait
     */
    public boolean processfastTopicRequest(RequestRecord request_record, int clientTimestamp) {
        DidaMeetingsCommand command = request_record.getRequest();
        
        if (command.getAction() != DidaMeetingsAction.TOPIC) {
            return false;
        }
        
        int meetingId = command.getMeetingId();
        int participantId = command.getParticipantId();
        int topicId = command.getTopicId();
        
        System.out.println("[FAST_TOPIC] Processing TOPIC request " + request_record.getId() + " (meeting:" + meetingId + ", participant:" + participantId + ", topic:" + topicId + ")");
        
        boolean meetingExists = serverState.meeting_manager.meetingExists(meetingId);
        boolean participantExists = false;
        if (meetingExists) {
            participantExists = serverState.meeting_manager.participantExists(meetingId, participantId);
        }
        System.out.println("[FAST_TOPIC] STEP 1: meeting exists=" + meetingExists + ", participant exists=" + participantExists);
        
        // If both meeting and participant exist, execute and respond TRUE
        if (meetingExists && participantExists) {
            boolean success = serverState.meeting_manager.setTopic(meetingId, participantId, topicId);
            System.out.println("[FAST_TOPIC] SUCCESS: Executed immediately with result=" + success);
            request_record.setResponse(success);
            return success;
        }
        System.out.println("[FAST_TOPIC] STEP 2 SKIP: Meeting or participant missing - checking timestamp");
        
        // Meeting or participant don't exist - check if we should respond now
        int currentTimestamp = serverState.getCurrentLogicalTimestamp();
        System.out.println("[FAST_TOPIC] STEP 3: Timestamp check - current:" + currentTimestamp + " vs client:" + clientTimestamp);
        
        if (currentTimestamp >= clientTimestamp) {
            boolean success = serverState.meeting_manager.setTopic(meetingId, participantId, topicId);
            System.out.println("[FAST_TOPIC] STEP 3 RESPONSE: Executed with result=" + success + " (could be false if dependencies missing)");
            request_record.setResponse(success);
            return success;
        }
        
        // Current timestamp < client timestamp - queue and wait for synchronization
        System.out.println("[FAST_TOPIC] STEP 4: Current timestamp < client timestamp - queuing for timestamp synchronization");
        try {
            CompletableFuture<Boolean> future = queuefastTopicRequest(request_record, clientTimestamp);
            return future.get(); // Wait for completion
        } catch (Exception e) {
            System.out.println("[FAST_TOPIC] Error in queued processing: " + e.getMessage());
            request_record.setResponse(false);
            return false;
        }
    }
    
    /** Queue a Fast Paxos request for sequential processing */
    public CompletableFuture<Boolean> queuefastTopicRequest(RequestRecord request_record, int clientTimestamp) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        fastTopicQueueItem item = new fastTopicQueueItem(request_record, clientTimestamp, future);
        
        System.out.println("[FAST_TOPIC] Queuing request " + request_record.getId() + " for sequential processing");
        
        try {
            fastTopicQueue.put(item);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            future.complete(false);
        }
        
        return future;
    }
    
    /** Worker thread that processes Fast Paxos requests sequentially */
    private void processfastTopicQueue() {
        System.out.println("[FAST_TOPIC] Fast Paxos worker thread started");
        
        while (!Thread.currentThread().isInterrupted()) {
            try {
                fastTopicQueueItem item = fastTopicQueue.take(); // Blocking call
                
                System.out.println("[FAST_TOPIC] Processing queued request " + item.requestRecord.getId());
                
                boolean result = processfastTopicRequestDirectly(item);
                item.future.complete(result);
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                System.out.println("[FAST_TOPIC] Error in worker thread: " + e.getMessage());
            }
        }
        
        System.out.println("[FAST_TOPIC] Fast Paxos worker thread stopped");
    }
    
    /**
     * Direct Fast Paxos processing (called by worker thread)
     * Phase 0: Wait for logical timestamp synchronization
     * Phase 1: Execute the command directly
     */
    private boolean processfastTopicRequestDirectly(fastTopicQueueItem item) {
        RequestRecord request_record = item.requestRecord;
        int clientTimestamp = item.clientTimestamp;
        DidaMeetingsCommand command = request_record.getRequest();
        
        if (command.getAction() != DidaMeetingsAction.TOPIC) {
            return false;
        }
        
        // 0: Wait for logical timestamp synchronization
        System.out.println("[FAST_TOPIC] PHASE 0: Waiting for logical timestamp " + clientTimestamp);
        
        // Mark as active waiting request for predictive execution
        activeWaitingRequest = item;
        
        int currentTimestamp = serverState.waitForLogicalTimestamp(clientTimestamp);
        
        // Clear active waiting request
        activeWaitingRequest = null;
        
        System.out.println("[FAST_TOPIC] PHASE 0 SUCCESS: Synchronized at timestamp " + currentTimestamp);

        // 1: Execute the command directly - logical timestamp sync ensures dependencies are resolved
        System.out.println("[FAST_TOPIC] Executing TOPIC command directly");
        boolean success = serverState.meeting_manager.setTopic(
            command.getMeetingId(), 
            command.getParticipantId(), 
            command.getTopicId()
        );
        
        if (success) {
            System.out.println("[FAST_TOPIC] SUCCESS: TOPIC command executed");
        } else {
            System.out.println("[FAST_TOPIC] FAILED: Command cannot execute with current state");
        }
        request_record.setResponse(success);
        return success;
    }

    /**
     * Called when an ADD request is received - check if it enables any waiting TOPIC requests
     * This allows us to respond early to TOPIC clients before the ADD is actually executed
     */
    public void notifyAddRequest(int meetingId, int participantId) {
        System.out.println("[FAST_TOPIC] ADD request notification: meeting=" + meetingId + ", participant=" + participantId);
        
        // Check if ADD will eventually succeed:
        // 1. Meeting already exists (either open or closed)
        // 2. OR there's a pending OPEN request for this meeting
        boolean meetingExists = serverState.meeting_manager.meetingExists(meetingId);
        boolean hasPendingOpen = serverState.req_history.hasPendingOpenRequest(meetingId);
        
        if (!meetingExists && !hasPendingOpen) {
            System.out.println("[FAST_TOPIC] Meeting " + meetingId + " doesn't exist and no pending OPEN - ADD won't help pending TOPIC requests");
            return;
        }
        
        System.out.println("[FAST_TOPIC] ADD request will eventually succeed (meeting exists: " + meetingExists + ", pending OPEN: " + hasPendingOpen + ")");
        
        // Check active waiting request
        fastTopicQueueItem activeItem = activeWaitingRequest;
        if (activeItem != null && !activeItem.predictiveResponseSent) {
            if (checkAndRespondPredictively(activeItem, meetingId, participantId)) {
                System.out.println("[FAST_TOPIC] Sent predictive response to active waiting request " + activeItem.requestRecord.getId());
            }
        }
        
        // Check queued requests
        for (fastTopicQueueItem item : fastTopicQueue) {
            if (!item.predictiveResponseSent) {
                if (checkAndRespondPredictively(item, meetingId, participantId)) {
                    System.out.println("[FAST_TOPIC] Sent predictive response to queued request " + item.requestRecord.getId());
                }
            }
        }
    }
    
    /**
     * Check if the ADD command enables this TOPIC request and respond predictively if so
     */
    private boolean checkAndRespondPredictively(fastTopicQueueItem item, int addMeetingId, int addParticipantId) {
        DidaMeetingsCommand command = item.requestRecord.getRequest();
        
        // Only applies to TOPIC commands
        if (command.getAction() != DidaMeetingsAction.TOPIC) {
            return false;
        }
        
        int topicMeetingId = command.getMeetingId();
        int topicParticipantId = command.getParticipantId();
        
        // Check if this ADD command enables the TOPIC request
        if (addMeetingId == topicMeetingId && addParticipantId == topicParticipantId) {
            System.out.println("[FAST_TOPIC] ADD enables TOPIC request - meeting:" + topicMeetingId + ", participant:" + topicParticipantId);
            
            // Send predictive response
            item.predictiveResponseSent = true;
            item.requestRecord.setResponse(true);
            item.future.complete(true);
            return true;
        }
        
        return false;
    }
}