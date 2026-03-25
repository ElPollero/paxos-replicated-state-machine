package didameetings.server;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import didameetings.DidaMeetingsPaxos;
import didameetings.util.CollectorStreamObserver;
import didameetings.util.GenericResponseCollector;
import didameetings.util.PhaseOneResponseProcessor;
import didameetings.util.PhaseTwoResponseProcessor;

import java.util.Map;

public class MainLoop implements Runnable  {
    
    DidaMeetingsServerState server_state;
    
    // Concurrent Multi-Paxos: Thread pool for concurrent instance processing
    private final ExecutorService instancePool;
    private final AtomicInteger activeInstances;
    private final int MAX_CONCURRENT_INSTANCES = 10; // Limit concurrent instances
    
    // Separate execution thread for command processing
    private final Thread executionThread;
    private volatile boolean shutdownRequested = false;
    
    
    public MainLoop(DidaMeetingsServerState state) {
	this.server_state   = state;
	
	// Initialize concurrent processing components
	this.instancePool = Executors.newFixedThreadPool(MAX_CONCURRENT_INSTANCES, 
	    r -> new Thread(r, "PaxosInstance-" + System.currentTimeMillis()));
	this.activeInstances = new AtomicInteger(0);
	
	// Initialize separate execution thread
	this.executionThread = new Thread(this::runExecutionLoop, "CommandExecutor");
	this.executionThread.setPriority(Thread.MAX_PRIORITY); // High priority for timely command execution
	this.executionThread.start();
	
	logInfo("CONCURRENT", "MainLoop initialized with max " + MAX_CONCURRENT_INSTANCES + " concurrent instances");
	logInfo("CONCURRENT", "Started separate command execution thread");
    }
    
    // Helper for cleaner logging
    private void logInfo(String category, String message) {
        System.out.println("[" + category + "] " + message);
    }

    @Override
    public void run() {
        logInfo("CONCURRENT", "Starting demand-driven concurrent Multi-Paxos MainLoop");
        
        while (true) {
            synchronized (this) {
                handleDebugMode();
                while (this.server_state.isFreezed()) {
                    try {
                        logInfo("DEBUG", "Server is frozen, waiting for unfreeze command...");
                        wait();
                        handleDebugMode();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }

            // Priority 1: Check for leadership change and perform recovery if needed.
            int ballot = this.server_state.getCurrentBallot();
            int designated_leader = this.server_state.scheduler.leader(ballot);

            if (designated_leader == this.server_state.my_id) {
                if (!this.server_state.isStableLeader() || this.server_state.getLeaderBallot() != ballot) {
                    logInfo("LEADER", "New leadership detected. Initiating leader establishment and log recovery.");
                    // Use dedicated leader establishment method that doesn't consume instance numbers
                    establishLeadershipAndRecover(ballot);
                }
            }

            // After potential recovery, check for pending requests to process.
            if (designated_leader == this.server_state.my_id && this.server_state.isStableLeader()) {
            
                // DEMAND-DRIVEN: Process ALL available requests concurrently (up to capacity)
                int requestsBatchProcessed = 0;
                
                while (activeInstances.get() < MAX_CONCURRENT_INSTANCES) {
                    RequestRecord pending_request = this.server_state.req_history.getFirstPending();
                    
                    if (pending_request == null) {
                        // No more pending requests
                        break;
                    }

                    logInfo("CONCURRENT", "Found pending request " + pending_request.getId() + " with instance " + pending_request.getInstanceNumber());
                    
                    // We have a request and capacity - start concurrent Paxos instance
                    final int currentInstance = pending_request.getInstanceNumber();
                    requestsBatchProcessed++;
                    
                    // CRITICAL: Move the request to "proposed" status IMMEDIATELY
                    if (this.server_state.req_history.isInProposed(pending_request.getId())) {
                        logInfo("CONCURRENT", "Request " + pending_request.getId() + " already in proposed state, skipping");
                        continue;
                    }
                    
                    this.server_state.req_history.moveToProposed(pending_request.getId());

                    logInfo("CONCURRENT", "Batch[" + requestsBatchProcessed + "] Request " + pending_request.getId() + " -> starting concurrent instance " + currentInstance);
                    
                    // Submit instance for concurrent processing
                    instancePool.submit(() -> {
                        int activeCount = activeInstances.incrementAndGet();
                        logInfo("CONCURRENT", "Processing instance " + currentInstance + 
                            " for request " + pending_request.getId() + " (active: " + activeCount + ")");
                        
                        try {
                            processEntry(currentInstance, pending_request);
                        } catch (Exception e) {
                            logInfo("ERROR", "Instance " + currentInstance + " failed: " + e.getMessage());
                        } finally {
                            int remainingCount = activeInstances.decrementAndGet();
                            logInfo("CONCURRENT", "Completed instance " + currentInstance + " (active: " + remainingCount + ")");
                            
                            // Notify MainLoop that capacity is available
                            synchronized (MainLoop.this) {
                                MainLoop.this.notify();
                            }
                        }
                    });
                }
                
                if (requestsBatchProcessed > 1) {
                    logInfo("CONCURRENT", "BATCH PROCESSING: Started " + requestsBatchProcessed + " concurrent instances");
                } else if (requestsBatchProcessed == 1) {
                    logInfo("CONCURRENT", "SINGLE REQUEST: Started 1 instance (no other requests pending)");
                }
            } else if (designated_leader != this.server_state.my_id) {
                // We're not the leader - don't process any instances, just wait for leader's decisions
                logInfo("LEADER", "Node " + this.server_state.my_id + " is not the leader (leader=" + designated_leader + ") - waiting for leader decisions");
                this.server_state.acknowledgeLeader(designated_leader, ballot);
            }
            
            // REACTIVE: Always sleep after processing - wake up on new requests or completed instances
            synchronized (this) {
                try {
                    logInfo("CONCURRENT", "Main loop cycle finished. Sleeping until next wakeup...");
                    wait(); 
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    public synchronized void wakeup() {
	    notify();    
    }
    
    private void handleDebugMode() {
        switch (this.server_state.getDebugMode()) {
            case 1 -> {
                logInfo("DEBUG", "Mode 1 - Crashing server");
                System.exit(1);
            }
            case 2 -> {
                logInfo("DEBUG", "Mode 2 - Freezing server");
                this.server_state.setFreezed(true);
            }
            case 3 -> {
                logInfo("DEBUG", "Mode 3 - Un-freezing server");
                this.server_state.setFreezed(false);
            }
            case 4 -> {
                logInfo("DEBUG", "Mode 4 - Slow mode ON");
                this.server_state.setSlowMode(true);
            }
            case 5 -> {
                logInfo("DEBUG", "Mode 5 - Slow mode OFF");
                this.server_state.setSlowMode(false);
            }
            default -> {
            }
        }
	}

    /**
     * Separate execution thread that processes commands in order
     * This runs independently of MainLoop, allowing MainLoop to stay responsive
     */
    private void runExecutionLoop() {
        logInfo("EXECUTION", "Command execution thread started");
        
        while (!shutdownRequested) {
            try {
                // Process all available decisions in order
                boolean processedAny = deliverInOrder();
                
                if (!processedAny) {
                    // No commands to execute - brief sleep
                    synchronized (this.server_state) {
                        this.server_state.wait(10); // Wake up on new decisions or timeout
                    }
                }
            } catch (InterruptedException e) {
                logInfo("EXECUTION", "Command execution thread interrupted");
                break;
            } catch (Exception e) {
                logInfo("ERROR", "Command execution error: " + e.getMessage());
                // Continue processing despite errors
            }
        }
        
        logInfo("EXECUTION", "Command execution thread stopped");
    }
    
    /**
     * Shutdown concurrent processing gracefully
     */
    public void shutdown() {
        logInfo("CONCURRENT", "Shutting down concurrent Multi-Paxos processing...");
        
        // Shutdown execution thread
        shutdownRequested = true;
        synchronized (this.server_state) {
            this.server_state.notify(); // Wake up execution thread to check shutdown
        }
        
        try {
            executionThread.join(5000); // Wait up to 5 seconds
            if (executionThread.isAlive()) {
                logInfo("CONCURRENT", "Force interrupting execution thread");
                executionThread.interrupt();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Shutdown instance pool
        instancePool.shutdown();
        try {
            if (!instancePool.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS)) {
                logInfo("CONCURRENT", "Force shutting down remaining instances");
                instancePool.shutdownNow();
            }
        } catch (InterruptedException e) {
            instancePool.shutdownNow();
            Thread.currentThread().interrupt();
        }
        logInfo("CONCURRENT", "Concurrent Multi-Paxos shutdown complete");
    }
    
    public void processEntry(int entry_number, RequestRecord specific_request) {
		
	PaxosInstance next_entry = this.server_state.paxos_log.testAndSetEntry(entry_number);

	while (next_entry.decided == false) {
	    RequestRecord request_record   = specific_request; // Use the specific request passed in
	    int           ballot           = this.server_state.getCurrentBallot ();

	    List<Integer> acceptors        = this.server_state.scheduler.acceptors (ballot);
	    int           quorum           = this.server_state.scheduler.quorum (ballot);
	    int           n_acceptors      = acceptors.size();
	    
	    // Multi-Paxos: Get designated leader from scheduler
	    int designated_leader = this.server_state.scheduler.leader(ballot);
        logInfo("INSTANCE", "Processing instance " + entry_number);
	    // Only stable leaders process requests - go directly to Phase 2
	    if ((ballot > -1) && (designated_leader == this.server_state.my_id)) {
		
			int phase_two_value = -1;

			if (request_record != null) {
				logInfo("PAXOS", "Processing request " + request_record.getId() + " (ballot=" + ballot + ")");
				phase_two_value = request_record.getId();
			}

		    // Update heartbeat since we're actively leading
		    this.server_state.updateLeaderHeartbeat();
		    
		    // Go directly to Phase 2 - leadership and log recovery already handled
		    logInfo("PAXOS", "Proceeding directly to Phase 2 with stable leadership (ballot=" + ballot + ")");
		    boolean phase_two_success = executePhaseTwo(entry_number, phase_two_value, ballot, acceptors, n_acceptors, quorum, false);
		    
		    if (phase_two_success) {
		        if (request_record != null) {
		            logInfo("PAXOS", "Request " + request_record.getId() + " decided in instance " + entry_number);
		        }
		    } else {
		        logInfo("PAXOS", "Phase 2 FAILED - Request " + (request_record != null ? request_record.getId() : "null") + " aborted in instance " + entry_number);
		    }
	    }
	    // For concurrent processing, don't wait indefinitely if not decided
	    if (next_entry.decided == false) {
            logInfo("CONCURRENT", "Instance " + entry_number + " timeout - exiting");
            return;
	    }
	}
	// A decisão agora é processada pela lógica de ordem total no deliverInOrder()
    logInfo("DELIVERY", "Delivering log entry " + entry_number + " with command_id " + (next_entry.decided ? next_entry.command_id : "pending"));
    }

    /** Aplica decisões por prefixo contíguo (1..N). Chamado pelo ExecutionThread. */
    private boolean deliverInOrder() {
        boolean processedAny = false;
        boolean hasNext = this.server_state.hasNextDecisionToDeliver();
        
        if (hasNext) {
            logInfo("EXECUTION", "Found decisions to deliver");
        }
        
        while (this.server_state.hasNextDecisionToDeliver()) {
            Integer value = this.server_state.getNextDecisionToDeliver();
            if (value == null) break;
            
            int instance = this.server_state.deliveredUpTo();
            logInfo("DELIVERY", "Delivering in order: instance " + instance + " with command_id " + value);
            applyLearned(instance, value);
            processedAny = true;
        }
        
        return processedAny;
    }

    /** Aplica o comando do 'value' (command_id) na state machine e completa a resposta do cliente. */
    private void applyLearned(int instance, int commandId) {
        if (commandId == -1) {
            logInfo("EXECUTE", "Applied no-op for instance " + instance + " (gap filling).");
            return; // Nothing to execute for a no-op.
        }

        RequestRecord request_record = this.server_state.req_history.getByCommandId(commandId);
        
        // Se o record ainda não chegou, espera
        while (request_record == null) {
            logInfo("WAIT", "Record for command_id " + commandId + " not available yet, waiting...");
            try {
                synchronized(this) {
                    wait(100); // timeout pequeno para não bloquear indefinidamente
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            request_record = this.server_state.req_history.getByCommandId(commandId);
        }

        DidaMeetingsCommand command = request_record.getRequest();
        boolean result = executeCommand(command);
        
        logInfo("EXECUTE", "Applied command with id " + commandId + " in instance " + instance + " with result " + result);
        
        // Only increment logical timestamp for OPEN and ADD commands (state-changing operations that affect TOPIC)
        DidaMeetingsAction action = command.getAction();
        int logicalTimestamp = 0;
        if (action == DidaMeetingsAction.OPEN || action == DidaMeetingsAction.ADD) {
            logicalTimestamp = this.server_state.incrementLogicalTimestamp();
            logInfo("TIMESTAMP", "Incremented logical timestamp to " + logicalTimestamp + " after instance " + instance + " (command: " + action + ")");
        } else {
            logInfo("TIMESTAMP", "No timestamp increment for " + action + " command - not relevant for Fast Paxos synchronization");
        }
        
        request_record.setResponse(result);
        request_record.setLogicalTimestamp(logicalTimestamp);
        this.server_state.req_history.moveToProcessed(request_record.getId());
    }

    /** Executa um comando na state machine. */
    private boolean executeCommand(DidaMeetingsCommand command) {
        boolean result = false;
        DidaMeetingsAction action = command.getAction();

        switch (action) {
            case DidaMeetingsAction.OPEN:
                logInfo("EXECUTE", "Executing OPEN request with id " + command.getMeetingId() + 
                                 " and max " + this.server_state.max_participants);
                result = this.server_state.meeting_manager.open(command.getMeetingId(), 
                                                              this.server_state.max_participants);
                break;
            case DidaMeetingsAction.ADD:
                result = this.server_state.meeting_manager.addAndClose(command.getMeetingId(), 
                                                                     command.getParticipantId());
                break;
            case DidaMeetingsAction.TOPIC:
                result = this.server_state.meeting_manager.setTopic(command.getMeetingId(), 
                                                                  command.getParticipantId(), 
                                                                  command.getTopicId());
                break;
            case DidaMeetingsAction.CLOSE:
                result = this.server_state.meeting_manager.close(command.getMeetingId());
                break;
            case DidaMeetingsAction.DUMP:
                this.server_state.meeting_manager.dump();
                result = true;
                break;
            default:
                result = false;
                System.err.println("*** Unknown command ****");
                break;
        }
        return result;
    }

    /** Phase 1 for leadership establishment and log recovery. */
    private void establishLeadershipAndRecover(int ballot) {
        logInfo("LEADER", "Starting dedicated leader establishment");
        
        // Establish leadership first
        this.server_state.establishLeadership(ballot);
        
        // Determine which config we need quorum from 
        List<Integer> previousConfigAcceptors;
        int previousConfigQuorum;
        
        if (ballot > 0) {
            if (ballot > 5){
                // After ballot 5, config never changes - use current config for quorum
                previousConfigQuorum = this.server_state.scheduler.quorum(ballot);
                previousConfigAcceptors = this.server_state.scheduler.acceptors(ballot);
                logInfo("VERTICAL_PAXOS", "Need quorum from current config (ballot " + ballot + ") - " + previousConfigQuorum + " out of " + previousConfigAcceptors.size());
            }else{
                // Use previous config for quorum
                previousConfigAcceptors = this.server_state.scheduler.acceptors(ballot - 1);
                previousConfigQuorum = this.server_state.scheduler.quorum(ballot - 1);
                logInfo("VERTICAL_PAXOS", "Need quorum from PREVIOUS config (ballot " + (ballot - 1) + ") - " + previousConfigQuorum + " out of " + previousConfigAcceptors.size());
            }
        } else {
            // First ballot - use current configuration
            previousConfigAcceptors = this.server_state.scheduler.acceptors(ballot);
            previousConfigQuorum = this.server_state.scheduler.quorum(ballot);
            logInfo("VERTICAL_PAXOS", "First ballot - need quorum from current config");
        }
        
        List<Integer> allServers = this.server_state.all_participants;
        int n_all_servers = allServers.size();
        
        logInfo("VERTICAL_PAXOS", "ASKING ALL SERVERS: Sending Phase 1 to ALL " + n_all_servers + " servers for complete log collection");
        
        // Wait for other servers to start up (only for initial leadership establishment)
        if (n_all_servers > 1 && ballot == 0 && this.server_state.my_id == 0) {
            try {
                logInfo("LEADER", "Initial startup: Waiting 2 seconds for other servers to start up...");
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        int completed_ballot = this.server_state.getCompletedBallot();
        logInfo("LEADER", "Running Phase 1 for leader establishment and log recovery ");
        
        // Create Phase 1 request for log recovery only (no specific instance)
        DidaMeetingsPaxos.PhaseOneRequest.Builder phase_one_request_builder = DidaMeetingsPaxos.PhaseOneRequest.newBuilder();
        phase_one_request_builder.setInstance(-1); // Use -1 to indicate this is log recovery only
        phase_one_request_builder.setRequestballot(ballot);
        phase_one_request_builder.setRequestLogRecovery(true);
        phase_one_request_builder.setFromInstance(0);

        DidaMeetingsPaxos.PhaseOneRequest phase_one_request = phase_one_request_builder.build();
        logInfo("LEADER", "Sending Phase 1 request for log recovery: " + phase_one_request);
        
        int low_ballot = Math.max(completed_ballot, 0);
        int high_ballot = ballot;
        
        PhaseOneResponseProcessor phase_one_processor = new PhaseOneResponseProcessor(this.server_state.scheduler, low_ballot, high_ballot);
        
        ArrayList<DidaMeetingsPaxos.PhaseOneReply> phase_one_responses = new ArrayList<DidaMeetingsPaxos.PhaseOneReply>();
        GenericResponseCollector<DidaMeetingsPaxos.PhaseOneReply> phase_one_collector = 
            new GenericResponseCollector<DidaMeetingsPaxos.PhaseOneReply>(phase_one_responses, n_all_servers, 
                                                                         phase_one_processor, previousConfigAcceptors, previousConfigQuorum);
        
        // Send Phase 1 to ALL servers
        for (int i = 0; i < n_all_servers; i++) {
            int serverId = allServers.get(i);
            
            CollectorStreamObserver<DidaMeetingsPaxos.PhaseOneReply> phase_one_observer = 
                new CollectorStreamObserver<DidaMeetingsPaxos.PhaseOneReply>(phase_one_collector);
            
            this.server_state.async_stubs[serverId]
                .withDeadlineAfter(100, java.util.concurrent.TimeUnit.SECONDS)
                .phaseone(phase_one_request, phase_one_observer);
        }
        
        // Main thread waits for PREVIOUS CONFIG quorum specifically
        logInfo("VERTICAL_PAXOS", "MAIN THREAD: Waiting for quorum of " + previousConfigQuorum + " responses from previous config: " + previousConfigAcceptors);
        phase_one_collector.waitForPreviousConfigQuorum();
        
        logInfo("PHASE1_RESPONSES", "Servers that responded:");
        List<DidaMeetingsPaxos.PhaseOneReply> responseSnapshot;
        synchronized (phase_one_responses) {
            responseSnapshot = new ArrayList<>(phase_one_responses);
        }
        for (DidaMeetingsPaxos.PhaseOneReply response : responseSnapshot) {
            logInfo("PHASE1_RESPONSES", "  Server " + response.getServerid() + ": accepted=" + response.getAccepted() + ", logs=" + response.getLogsCount());
        }
        
        // Log thread waits for ALL logs in background
        logInfo("VERTICAL_PAXOS", "LOG THREAD: Starting background collection for ALL " + n_all_servers + " servers...");
        startBackgroundLogCollection(phase_one_collector, phase_one_processor, n_all_servers);
               
        // Log details about collected logs
        if (phase_one_processor.hasCollectedLogs()) {
            Map<Integer, List<didameetings.DidaMeetingsPaxos.InstanceLog>> collectedLogs = phase_one_processor.getCollectedLogs();
            int totalLogEntries = collectedLogs.values().stream().mapToInt(List::size).sum();
            logInfo("LEADER", "Collected logs from " + collectedLogs.size() + " acceptors with " + totalLogEntries + " total log entries");
        }
        
        if (!phase_one_processor.hasQuorumOk()) {
            logInfo("LEADER", "Phase 1 FAILED - Leadership establishment failed");
            int maxballot = phase_one_processor.getMaxballot();
            if (maxballot > this.server_state.getCurrentBallot()) {
                this.server_state.setCurrentBallot(maxballot);
            }
            this.server_state.resetLeadership();
            return;
        }
        // Process received logs if any were collected
        if (phase_one_processor.hasCollectedLogs()) {
            logInfo("LEADER", "Processing logs collected during leader establishment");
            // Use previous config for recovery operations
            processReceivedLogsFromProcessor(phase_one_processor.getCollectedLogs(), ballot, 
                                           previousConfigAcceptors, previousConfigAcceptors.size(), previousConfigQuorum);
        }
        
        // Confirm leadership - no Phase 2 needed since this is just for log recovery
        this.server_state.confirmLeadership();
        logInfo("LEADER", "Leadership established and confirmed - now stable leader");
    }

    /** Simple wrapper to process logs collected by PhaseOneResponseProcessor */
    private void processReceivedLogsFromProcessor(Map<Integer, List<didameetings.DidaMeetingsPaxos.InstanceLog>> logsByAcceptor,
                                                int ballot, List<Integer> acceptors, int n_acceptors, int quorum) {
        if (logsByAcceptor.isEmpty()) {
            return;
        }
        // Determine starting ballot for Vertical Paxos analysis
        int startingBallot;
        if (ballot > 5) {
            // After ballot 5, config never changes - start from current ballot
            startingBallot = ballot;
        } else if (ballot > 0) {
            // Start from previous ballot (the configuration we queried)
            startingBallot = ballot - 1;
        } else {
            startingBallot = ballot;
        }
        
        logInfo("VERTICAL_PAXOS", "Starting Vertical Paxos analysis from ballot " + startingBallot + " (current ballot: " + ballot + ")");
        
        // Analyze logs and determine recovery actions
        java.util.List<didameetings.server.DidaMeetingsServerState.RecoveryAction> recoveryActions = 
            this.server_state.analyzeLogsForRecoveryWithMap(logsByAcceptor, startingBallot);
            
        // The goal is to get the current config acceptors to create logs for these instances
        List<Integer> currentConfigAcceptors = this.server_state.scheduler.acceptors(ballot);
        int currentConfigQuorum = this.server_state.scheduler.quorum(ballot);
        int currentConfigSize = currentConfigAcceptors.size();
        
        logInfo("RECOVERY", "Using CURRENT config for Phase 2 recovery: acceptors=" + currentConfigAcceptors + ", quorum=" + currentConfigQuorum + " (ballot " + ballot + ")");
        
        boolean allRecoverySuccessful = true;
        for (didameetings.server.DidaMeetingsServerState.RecoveryAction action : recoveryActions) {
            boolean success = executePhaseTwo(action.instance, action.value, ballot, 
                                            currentConfigAcceptors, currentConfigSize, currentConfigQuorum, true);
            if (!success) {
                allRecoverySuccessful = false;
                logInfo("RECOVERY", "Recovery failed for instance " + action.instance + " - cannot update completed ballot");
            }
        }
        
        // CHECKPOINT: Update completed ballot only if ALL recovery was successful
        if (allRecoverySuccessful && recoveryActions.size() > 0) {
            // Update completed ballot to the starting ballot we just recovered
            this.server_state.updateCompletedBallot(ballot);
            logInfo("CHECKPOINT", "Successfully recovered from ballot " + startingBallot + " - updated completed ballot to " + ballot + " (recovered " + recoveryActions.size() + " instances)");
        } else if (recoveryActions.size() > 0) {
            logInfo("RECOVERY", "Some recovery actions failed - NOT updating completed ballot");
        }
        
        logInfo("RECOVERY", "Completed recovery for " + recoveryActions.size() + " instances");
    }

    /** Executes Phase 2 of Paxos for a given instance with unified handling for both normal operations and recovery scenarios. */
    private boolean executePhaseTwo(int instance, int value, int ballot, List<Integer> acceptors, int n_acceptors, int quorum, boolean isRecovery) {
        String logPrefix = isRecovery ? "RECOVERY" : "PAXOS";
        logInfo(logPrefix, "Running Phase 2 for instance " + instance + " with value " + value);
        
        // Build Phase 2 request
        DidaMeetingsPaxos.PhaseTwoRequest.Builder phase_two_request = DidaMeetingsPaxos.PhaseTwoRequest.newBuilder();
        phase_two_request.setInstance(instance);
        phase_two_request.setRequestballot(ballot);
        phase_two_request.setValue(value);

        PhaseTwoResponseProcessor phase_two_processor = new PhaseTwoResponseProcessor(quorum);
        
        ArrayList<DidaMeetingsPaxos.PhaseTwoReply> phase_two_responses = new ArrayList<DidaMeetingsPaxos.PhaseTwoReply>();
        GenericResponseCollector<DidaMeetingsPaxos.PhaseTwoReply> phase_two_collector = 
            new GenericResponseCollector<DidaMeetingsPaxos.PhaseTwoReply>(phase_two_responses, n_acceptors, phase_two_processor);
        
        // Send Phase 2 to all acceptors
        for (int i = 0; i < n_acceptors; i++) {
            CollectorStreamObserver<DidaMeetingsPaxos.PhaseTwoReply> phase_two_observer = 
                new CollectorStreamObserver<DidaMeetingsPaxos.PhaseTwoReply>(phase_two_collector);
            this.server_state.async_stubs[acceptors.get(i)]
                .withDeadlineAfter(100, java.util.concurrent.TimeUnit.SECONDS)
                .phasetwo(phase_two_request.build(), phase_two_observer);
        }

        // Wait for responses and process result
        phase_two_collector.waitUntilDone();
        
        if (phase_two_processor.getAccepted()) {
            logInfo(logPrefix, "Instance " + instance + " Phase 2 SUCCESS - completed with value " + value);
            this.server_state.setCompletedBallot(ballot);
            
            // Mark this instance as decided
            this.server_state.onDecided(instance, value);
            
            return true;
        } else {
            logInfo(logPrefix, "Instance " + instance + " Phase 2 FAILED - did not achieve quorum");
            if (!isRecovery) {
                // For normal operations, update ballot and reset leadership on failure
                this.server_state.setCurrentBallot(phase_two_processor.getMaxballot());
                this.server_state.resetLeadership();
            }
            return false;
        }
    }
    
    /** Background thread that waits for ALL logs and stores them by server ID */
    private void startBackgroundLogCollection(GenericResponseCollector<DidaMeetingsPaxos.PhaseOneReply> collector,
                                             PhaseOneResponseProcessor processor,
                                             int totalServers) {
        Thread logCollectionThread = new Thread(() -> {
            try {
                logInfo("LOG_THREAD", "Waiting for ALL " + totalServers + " server responses...");
                
                // Accelerated collection with fast polling
                long startTime = System.currentTimeMillis();
                long timeoutMs = 2000; // 5 second timeout
                
                synchronized (collector) {
                    while (!isCollectionComplete(collector, totalServers) && 
                           (System.currentTimeMillis() - startTime) < timeoutMs) {
                        try {
                            collector.wait(50); // Check every 500ms
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
                // Store logs immediately for cross-config analysis
                Map<Integer, List<didameetings.DidaMeetingsPaxos.InstanceLog>> allLogs = processor.getCollectedLogs();
                java.util.Set<Integer> respondingServers = processor.getRespondingServers();
                
                this.server_state.storeCompleteLogsByServerId(allLogs);
                
                long duration = System.currentTimeMillis() - startTime;
                logInfo("LOG_THREAD", "ACCELERATED collection COMPLETE - " + respondingServers.size() + "/" + totalServers + " servers in " + duration + "ms");
                logInfo("LOG_THREAD", "Cross-config logs ready: " + respondingServers);
                       
            } catch (Exception e) {
                logInfo("LOG_THREAD", "Background log collection failed: " + e.getMessage());
            }
        }, "LogCollectionThread");
        
        logCollectionThread.setDaemon(true);
        logCollectionThread.start();
    }
    
    /** Helper method to check if log collection is complete */
    private boolean isCollectionComplete(GenericResponseCollector<DidaMeetingsPaxos.PhaseOneReply> collector, 
                                       int expectedResponses) {
        return collector.isDone() || collector.getResponseCount() >= expectedResponses;
    }
}
   