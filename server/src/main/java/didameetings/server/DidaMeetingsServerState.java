package didameetings.server;

import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import didameetings.DidaMeetingsPaxosServiceGrpc;
import didameetings.configs.ConfigurationScheduler;
import didameetings.core.MeetingManager;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;


public class DidaMeetingsServerState {
    // Special marker value for fake leader establishment requests (should not create instances)
    
    int                         max_participants;
    MeetingManager              meeting_manager;
    ConfigurationScheduler      scheduler;
    int                         base_port;
    int                         my_id;
    RequestHistory              req_history;
    PaxosLog                    paxos_log;

    List<Integer>               all_participants;
    int                         n_participants;
    String[]                    targets;
    ManagedChannel[]            channels;
    DidaMeetingsPaxosServiceGrpc.DidaMeetingsPaxosServiceStub[] async_stubs;

    // Multi-Paxos instance counter - thread-safe for network reception
    private final AtomicInteger next_log_entry = new AtomicInteger(-1);

    // Logical timestamp for Fast Paxos synchronization - thread-safe counter
    private final AtomicInteger logical_timestamp = new AtomicInteger(0);
    private final Object        logical_timestamp_lock = new Object();

	// Multi-Paxos Leader State
    private int                 current_leader = -1;          // ID of current leader (-1 if none)
    private int                 leader_ballot = -1;           // Ballot number for current leadership
    private long                last_leader_heartbeat = 0;    // Timestamp of last heartbeat from leader
    private boolean             is_stable_leader = false;     // True if this node is the stable leader
    private long                leadership_start_time = 0;    // When this node became leader
    private static final long   LEADER_HEARTBEAT_INTERVAL = 1000; // 1 second
    private static final long   LEADER_TIMEOUT = 3000;       // 3 seconds without heartbeat = leader failure

    private int                 current_ballot;
    private int                 completed_ballot;
    private int                 debug_mode;

    // Dedicated locks to prevent deadlocks
    private final Object        completed_ballot_lock = new Object();
    private final Object        leadership_lock = new Object();

	boolean             		freezed = false;
	boolean             		slow_mode = false;
    
    // === Ordem total ===
    private final ConcurrentSkipListMap<Integer,Integer> decided = new ConcurrentSkipListMap<>();
    private final AtomicInteger lastDelivered = new AtomicInteger(-1);
 
    MainLoop                    main_loop;
    Thread                      main_loop_worker;
    
    public DidaMeetingsServerState(int port, int myself, char schedule, int max) {
	this.max_participants = max;
	this.meeting_manager  = new MeetingManager();
	this.scheduler        = new ConfigurationScheduler (schedule);
	this.base_port        = port;
	this.my_id            = myself;
	this.debug_mode       = 0;
	this.current_ballot   = 0;
	this.completed_ballot = -1;
	this.req_history      = new RequestHistory();
	this.paxos_log        = new PaxosLog();
	this.main_loop        = new MainLoop(this);

	// init comms
	this.all_participants = this.scheduler.allparticipants ();
	this.n_participants = all_participants.size();
	
	this.targets = new String[this.n_participants];
	for (int i = 0; i < this.n_participants; i++) {
	    int target_port = this.base_port + all_participants.get(i);
	    this.targets[i] = new String();
	    this.targets[i] = "localhost:" + target_port;
	    System.out.printf("targets[%d] = %s%n", i, targets[i]);
	}
	
	this.channels = new ManagedChannel[this.n_participants];
	for (int i = 0; i < this.n_participants; i++) 
	    this.channels[i] = ManagedChannelBuilder.forTarget(this.targets[i]).usePlaintext().build();
	
	this.async_stubs = new DidaMeetingsPaxosServiceGrpc.DidaMeetingsPaxosServiceStub[this.n_participants];
	for (int i = 0; i < this.n_participants; i++) 
	    this.async_stubs[i] = DidaMeetingsPaxosServiceGrpc.newStub(this.channels[i]);	

	// start worker
	this.main_loop_worker = new Thread (main_loop);
	this.main_loop_worker.start();
    }

    // Helper for cleaner logging
    private void logInfo(String category, String message) {
        System.out.println("[" + category + "] " + message);
    }

    public synchronized int getCurrentBallot () {
	return this.current_ballot;
    }
    
    public synchronized void setCurrentBallot (int ballot) {
	if (ballot > this.current_ballot) {
	    this.current_ballot = ballot;
	    
	    // Check if we're now the designated leader for this ballot
	    int designated_leader = this.scheduler.leader(ballot);
	    if (designated_leader == this.my_id) {
	        // We should be the leader - establish leadership immediately
	        if (!this.isStableLeader() || this.getLeaderBallot() != ballot) {
	            logInfo("LEADER", "Ballot change detected - establishing leadership for ballot " + ballot);
	            this.establishLeadership(ballot);
	        }
	    }
	}
    }

    public synchronized int getCompletedBallot () {
	return this.completed_ballot;
    }
    
    public void updateCompletedBallot (int ballot) {
        synchronized (completed_ballot_lock) {
            // CHECKPOINT: Update completed ballot to mark this ballot as safely recovered
            if (ballot > this.completed_ballot) {
                logInfo("CHECKPOINT", "Updating completed ballot from " + this.completed_ballot + " to " + ballot);
                this.completed_ballot = ballot;
            } else {
                logInfo("CHECKPOINT", "Not updating completed ballot - " + ballot + " <= current " + this.completed_ballot);
            }
            completed_ballot_lock.notifyAll();
        }
    }

    
    public void setCompletedBallot (int ballot) {
        synchronized (completed_ballot_lock) {
            if (ballot > this.completed_ballot)
                this.completed_ballot = ballot;
            completed_ballot_lock.notifyAll();
        }
    }

    
    public int waitForCompletedBallot(int ballot) {
        synchronized (completed_ballot_lock) {
            while (this.completed_ballot < ballot) {
                try {
                    completed_ballot_lock.wait();
                } catch (InterruptedException e) {
                }
            }
            return this.completed_ballot;
        }
    }

    // Logical timestamp methods for Fast Paxos synchronization
    public int getCurrentLogicalTimestamp() {
        return logical_timestamp.get();
    }

    public int incrementLogicalTimestamp() {
        synchronized (logical_timestamp_lock) {
            int newTimestamp = logical_timestamp.incrementAndGet();
            logical_timestamp_lock.notifyAll();
            return newTimestamp;
        }
    }

    public int waitForLogicalTimestamp(int targetTimestamp) {
        synchronized (logical_timestamp_lock) {
            while (logical_timestamp.get() < targetTimestamp) {
                try {
                    logical_timestamp_lock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return logical_timestamp.get();
                }
            }
            return logical_timestamp.get();
        }
    }

    public synchronized int getDebugMode () {
	return this.debug_mode;
    }

	public synchronized void setFreezed(boolean state) {
		this.freezed = state;
		notifyAll();
	}

	public synchronized boolean isFreezed() {
		return this.freezed;
	}

	public synchronized void setSlowMode(boolean state) {
		this.slow_mode = state;
	}

	public synchronized boolean isSlowMode() {
		return this.slow_mode;
	}

	public void applySlowModeDelay() {
		if (this.isSlowMode()) {
			try {
				int minDelay = 100;
				int maxDelay = 2000;
				int delay = new java.util.Random().nextInt(maxDelay - minDelay + 1) + minDelay;
				logInfo("SLOW MODE", "Applying " + delay + "ms delay");
				Thread.sleep(delay);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}

    public synchronized void setDebugMode (int mode) {
	this.debug_mode = mode;
    }

	// Multi-Paxos Leader Management Methods
    
    public synchronized boolean isStableLeader() {
        return this.is_stable_leader && this.current_leader == this.my_id;
    }
    
    public synchronized boolean hasValidLeader() {
        if (this.current_leader == -1) return false;
        
        // If we're the leader, we're always valid
        if (this.current_leader == this.my_id) return true;
        
        // Check if leader heartbeat is recent enough
        long now = System.currentTimeMillis();
        return (now - this.last_leader_heartbeat) < LEADER_TIMEOUT;
    }
    
    public synchronized int getCurrentLeader() {
        return this.current_leader;
    }
    
    public synchronized void establishLeadership(int ballot) {
        if (ballot >= this.leader_ballot) {
            // Reset any previous leadership state before establishing new leadership
            if (this.current_leader != this.my_id || this.leader_ballot != ballot) {
                this.is_stable_leader = false;
            }
            
            this.current_leader = this.my_id;
            this.leader_ballot = ballot;
            this.leadership_start_time = System.currentTimeMillis();
            this.last_leader_heartbeat = this.leadership_start_time;
            
            logInfo("LEADER", "Node " + this.my_id + " established leadership with ballot " + ballot + " (not stable yet)");
        }
    }
    
    public void confirmLeadership() {
        synchronized (leadership_lock) {
            if (this.current_leader == this.my_id && !this.is_stable_leader) {
                this.is_stable_leader = true;
                logInfo("LEADER", "Node " + this.my_id + " confirmed stable leadership for ballot " + this.leader_ballot);
                
                // Recovery will be handled by MainLoop when needed
            }
        }
    }
    
    public synchronized void acknowledgeLeader(int leader_id, int ballot) {
        if (ballot >= this.leader_ballot) {
            this.current_leader = leader_id;
            this.leader_ballot = ballot;
            this.is_stable_leader = false;
            this.last_leader_heartbeat = System.currentTimeMillis();
            logInfo("LEADER", "Node " + this.my_id + " acknowledged leader " + leader_id + " with ballot " + ballot);
        }
    }
    
    public synchronized void updateLeaderHeartbeat() {
        this.last_leader_heartbeat = System.currentTimeMillis();
    }
    
    public synchronized void resetLeadership() {
        this.current_leader = -1;
        this.leader_ballot = -1;
        this.is_stable_leader = false;
        this.leadership_start_time = 0;
        this.last_leader_heartbeat = 0;
        logInfo("LEADER", "Node " + this.my_id + " reset leadership state");
    }
    
    public synchronized boolean needsLeadershipRefresh() {
        if (!this.is_stable_leader) return false;
        
        long now = System.currentTimeMillis();
        return (now - this.last_leader_heartbeat) >= LEADER_HEARTBEAT_INTERVAL;
    }

    public synchronized int getLeaderBallot() {
        return this.leader_ballot;
    }

    // === Multi-Paxos com ordem total ===
    
    public int deliveredUpTo() { 
        return lastDelivered.get(); 
    }

    /** Chama isto quando a instância 'inst' for considerada decidida (quórum de accepts). */
    public void onDecided(int inst, int value) {
        // Guard against duplicate decisions
        if (decided.containsKey(inst)) {
            // Instance already decided - skip duplicate
            return;
        }
        
        logInfo("DEBUG", "onDecided: inst=" + inst + ", value=" + value);
        PaxosInstance e = paxos_log.testAndSetEntry(inst);
        synchronized (e) {
            e.decided = true;
            e.command_id = value;
            e.notifyAll(); // Wake up any threads waiting on this entry
        }
        decided.put(inst, value);
        // Wake up the execution thread to process commands in order
        synchronized (this) {
            this.notify(); // Notify execution thread
        }
    }
    

    /** Verifica se há decisões contíguas para entregar. Chamado pelo ExecutionThread. */
    public synchronized boolean hasNextDecisionToDeliver() {
        int next = lastDelivered.get() + 1;
        boolean hasNext = decided.containsKey(next);
        // Removed debug logging - this method is called frequently by execution thread
        return hasNext;
    }

    /** Retorna a próxima decisão a ser entregue em ordem, ou null se não houver. */
    public synchronized Integer getNextDecisionToDeliver() {
        int next = lastDelivered.get() + 1;
        Integer value = decided.get(next);
        if (value != null) {
            decided.remove(next);
            lastDelivered.incrementAndGet();
        }
        return value;
    }

    /** 
     * Assigns the next available instance number in reception order.
     * This ensures instance assignment follows request arrival order.
     */
    public int assignNextInstance() {
        return next_log_entry.incrementAndGet();
    }
    
    /** Get the current instance number (next instance to be assigned - 1) */
    public int getCurrentInstanceNumber() {
        return next_log_entry.get();
    }

    /**
     * Returns all log entries for log recovery.
     * Used by acceptors to send complete logs to new leaders.
     */
    public java.util.List<didameetings.DidaMeetingsPaxos.InstanceLog> getAllLogEntries() {
        java.util.List<didameetings.DidaMeetingsPaxos.InstanceLog> logs = new java.util.ArrayList<>();
        int length = this.paxos_log.length();
        
        for (int i = 0; i < length; i++) {
            PaxosInstance entry = this.paxos_log.getEntry(i);
            
            // Include instances that completed Phase 2 (write_ballot set) - acceptors who participated in Phase 2
            // Exclude entries that are just learned (accept_ballot set but write_ballot not set)
            if (entry != null && entry.write_ballot >= 0 && entry.command_id != 0) {
                didameetings.DidaMeetingsPaxos.InstanceLog.Builder logBuilder = 
                    didameetings.DidaMeetingsPaxos.InstanceLog.newBuilder();
                logBuilder.setInstance(i);
                logBuilder.setHighestAcceptedBallot(entry.write_ballot);
                logBuilder.setAcceptedValue(entry.command_id);
                logs.add(logBuilder.build());
            }
        }
        
        logInfo("LOG_RECOVERY", "Sending " + logs.size() + " log entries for recovery");
        return logs;
    }

    /**
     * Recovery action for a specific instance based on distributed log analysis
     */
    public static class RecoveryAction {
        public final int instance;
        public final int value;
        public final boolean hasConsensus;
        public final String reason;
        
        public RecoveryAction(int instance, int value, boolean hasConsensus, String reason) {
            this.instance = instance;
            this.value = value;
            this.hasConsensus = hasConsensus;
            this.reason = reason;
        }
    }

    /**
     * VERTICAL PAXOS: Analyzes logs recursively through ballot configurations.
     * For each instance, recursively analyzes logs from acceptors of each ballot found,
     * until no new ballots are discovered.
     */
    public java.util.List<RecoveryAction> analyzeLogsForRecoveryWithMap(
            java.util.Map<Integer, java.util.List<didameetings.DidaMeetingsPaxos.InstanceLog>> logsByAcceptor,
            int startingBallot) {
        
        logInfo("VERTICAL_PAXOS", "=== STARTING VERTICAL PAXOS RECURSIVE LOG ANALYSIS ===");
        
        // Log contributing acceptors
        for (java.util.Map.Entry<Integer, java.util.List<didameetings.DidaMeetingsPaxos.InstanceLog>> entry : logsByAcceptor.entrySet()) {
            logInfo("VERTICAL_PAXOS", "Acceptor " + entry.getKey() + " contributed " + entry.getValue().size() + " log entries");
        }
        
        // Group all logs by instance
        java.util.Map<Integer, java.util.List<didameetings.DidaMeetingsPaxos.InstanceLog>> instanceMap = 
            new java.util.HashMap<>();
            
        for (java.util.List<didameetings.DidaMeetingsPaxos.InstanceLog> acceptorLogs : logsByAcceptor.values()) {
            for (didameetings.DidaMeetingsPaxos.InstanceLog log : acceptorLogs) {
                instanceMap.computeIfAbsent(log.getInstance(), _ -> new java.util.ArrayList<>()).add(log);
            }
        }
        
        logInfo("VERTICAL_PAXOS", "Found " + instanceMap.size() + " instances in logs");
        
        // Analyze each instance recursively - check ALL instances up to current log length
        java.util.List<RecoveryAction> recoveryActions = new java.util.ArrayList<>();
        int maxInstance = determineMaxInstanceToAnalyze(instanceMap);
                
        for (int instance = 0; instance < maxInstance; instance++) {
            
            RecoveryAction action = null;
            if (instanceMap.containsKey(instance)) {
                // Instance has logs - analyze normally for recovery
                action = analyzeInstanceRecursively(instance, logsByAcceptor, startingBallot);
            } else {
                // Instance has no logs but is within distributed range - fill gap with no-op
                logInfo("VERTICAL_PAXOS", "Instance " + instance + " has no logs but within distributed range - filling gap with no-op");
                action = new RecoveryAction(instance, -1, true, "GAP_IN_DISTRIBUTED_LOGS - filling missing instance with no-op value -1");
            }
            if (action != null) {
                recoveryActions.add(action);
            }
        }
        
        logInfo("VERTICAL_PAXOS", "=== VERTICAL PAXOS ANALYSIS COMPLETE ===");
        logInfo("RECOVERY", "Analysis complete - " + recoveryActions.size() + " instances need recovery");
        
        return recoveryActions;
    }
    
    /**
     * CORE RECURSIVE ANALYSIS: Analyzes an instance by recursively discovering 
     * and analyzing all ballots found in the logs.
     */
    private RecoveryAction analyzeInstanceRecursively(int instance, 
            java.util.Map<Integer, java.util.List<didameetings.DidaMeetingsPaxos.InstanceLog>> logsByAcceptor,
            int currentBallot) {
            
            java.util.Set<Integer> analyzedBallots = new java.util.HashSet<>();
            java.util.Set<Integer> ballotsToAnalyze = new java.util.HashSet<>();
            
            // CHECKPOINT: Don't analyze if starting ballot is already completed (safe)
            if (currentBallot < this.completed_ballot) {
                logInfo("CHECKPOINT", "Skipping analysis for instance " + instance + " starting from ballot " + currentBallot + " - before completed ballot " + this.completed_ballot + " (already safe)");
                return null; // No recovery needed - already safe
            }
            
            // Start with current ballot
            ballotsToAnalyze.add(currentBallot);
            logInfo("VERTICAL_PAXOS", "Starting recursive analysis for instance " + instance + " from ballot " + currentBallot + " (completed ballot: " + this.completed_ballot + ")");
            
            RecoveryAction finalDecision = null;
            
            int currentAnalizedBallot = limitBallot(currentBallot);
            // Recursively analyze ballots until no new ones are discovered
            while (!ballotsToAnalyze.isEmpty()) {
                // Get next ballot to analyze
                if (ballotsToAnalyze.contains(currentAnalizedBallot)) {
                ballotsToAnalyze.remove(currentAnalizedBallot);
            }
            analyzedBallots.add(currentAnalizedBallot);
            
            // Get acceptors for this ballot's configuration BEFORE decrementing
            java.util.List<Integer> configAcceptors = this.scheduler.acceptors(currentAnalizedBallot);
            int configQuorum = this.scheduler.quorum(currentAnalizedBallot);
            
            logInfo("VERTICAL_PAXOS", "Config acceptors: " + configAcceptors + ", quorum: " + configQuorum);
            
            // Collect logs for this instance from this configuration's acceptors
            java.util.List<didameetings.DidaMeetingsPaxos.InstanceLog> configLogs = 
                new java.util.ArrayList<>();
            java.util.Set<Integer> newBallots = new java.util.HashSet<>();
            
            // SMART LOG RETRIEVAL: Try to get logs from stored complete collection
            java.util.Map<Integer, java.util.List<didameetings.DidaMeetingsPaxos.InstanceLog>> configSpecificLogs = 
                getLogsFromServers(configAcceptors);
                
            if (!configSpecificLogs.isEmpty()) {
                logInfo("VERTICAL_PAXOS", "Using COMPLETE stored logs for " + configSpecificLogs.size() + " out of " + configAcceptors.size() + " config acceptors");
            }
            
            // OPTIMIZATION: Track values as we collect to enable early quorum detection
            java.util.Map<Integer, Integer> valueCountEarly = new java.util.HashMap<>();
            boolean foundQuorum = false;
            
            for (Integer acceptorId : configAcceptors) {
                // EARLY TERMINATION: Stop once we have a quorum for any value
                if (foundQuorum) {
                    logInfo("VERTICAL_PAXOS", "EARLY TERMINATION: Already found quorum, skipping remaining acceptors");
                    break;
                }
                
                java.util.List<didameetings.DidaMeetingsPaxos.InstanceLog> acceptorLogs = null;
                
                // Try complete logs first, then fall back to initial logs
                if (configSpecificLogs.containsKey(acceptorId)) {
                    acceptorLogs = configSpecificLogs.get(acceptorId);
                    logInfo("VERTICAL_PAXOS", "Found acceptor " + acceptorId + " logs in COMPLETE storage");
                } else if (logsByAcceptor.containsKey(acceptorId)) {
                    acceptorLogs = logsByAcceptor.get(acceptorId);
                } else {
                    continue;
                }
                
                // Process the logs we found
                if (acceptorLogs != null) {
                    for (didameetings.DidaMeetingsPaxos.InstanceLog log : acceptorLogs) {
                        if (log.getInstance() == instance && log.getAcceptedValue() > 0) {
                            configLogs.add(log);
                            
                            // Track value counts for early quorum detection
                            int value = log.getAcceptedValue();
                            valueCountEarly.put(value, valueCountEarly.getOrDefault(value, 0) + 1);
                            
                            // Check if we've reached quorum for this value
                            if (valueCountEarly.get(value) >= configQuorum) {
                                foundQuorum = true;
                            }
                            
                            // Discover new ballots for recursive analysis
                            int logBallot = log.getHighestAcceptedBallot();
                            
                            // CHECKPOINT: Don't analyze ballots before completed_ballot (they're already safe)
                            if (logBallot < this.completed_ballot) {
                                logInfo("CHECKPOINT", "Skipping ballot " + logBallot + " - before completed ballot " + this.completed_ballot + " (already safe)");
                                continue;
                            }
                            
                            if (!analyzedBallots.contains(logBallot) && !ballotsToAnalyze.contains(logBallot)) {
                                logInfo("VERTICAL_PAXOS", "Discovered new ballot " + logBallot + " from acceptor " + acceptorId);
                                newBallots.add(logBallot);
                            }
                            logInfo("VERTICAL_PAXOS", "Acceptor " + acceptorId + " log: ballot=" + logBallot + ", value=" + log.getAcceptedValue());
                            // Break out of inner log loop if we found quorum
                            if (foundQuorum) break;
                        }
                    }
                }
            }
            
            // Add newly discovered ballots for analysis
            ballotsToAnalyze.addAll(newBallots);
            
            logInfo("VERTICAL_PAXOS", "Found " + configLogs.size() + " logs, discovered " + newBallots.size() + " new ballots: " + newBallots);
            
            // Analyze consensus in this configuration
            if (!configLogs.isEmpty()) {
                RecoveryAction configDecision = analyzeCollectedLogs(instance, configLogs, configAcceptors, configQuorum, currentAnalizedBallot, valueCountEarly);
                
                if (configDecision != null) {
                    logInfo("VERTICAL_PAXOS", "Decision in ballot " + currentAnalizedBallot + ": " + configDecision.reason);
                    
                    // Keep the decision with highest ballot (most recent)
                    if (finalDecision == null || currentAnalizedBallot > extractBallotFromReason(finalDecision.reason)) {
                        finalDecision = configDecision;
                    }
                }
            }
        }
        currentAnalizedBallot -= 1;
        logInfo("VERTICAL_PAXOS", "Recursive analysis complete for instance " + instance + ". Analyzed ballots: " + analyzedBallots);
        return finalDecision;
    }

    /** CONSENSUS ANALYZER: Uses already-collected logs and vote counts to make decisions. */
    private RecoveryAction analyzeCollectedLogs(int instance, 
            java.util.List<didameetings.DidaMeetingsPaxos.InstanceLog> configLogs,
            java.util.List<Integer> configAcceptors, int quorum, int configBallot,
            java.util.Map<Integer, Integer> alreadyCountedValues) {
        
        // Use the vote counts already collected during recursive analysis
        java.util.Map<Integer, Integer> valueCount = new java.util.HashMap<>(alreadyCountedValues);
        int respondingAcceptors = configLogs.size();
        
        // Find highest ballot value for fallback decisions
        int highestBallot = -1;
        int highestBallotValue = -1;
        for (didameetings.DidaMeetingsPaxos.InstanceLog log : configLogs) {
            if (log.getHighestAcceptedBallot() > highestBallot) {
                highestBallot = log.getHighestAcceptedBallot();
                highestBallotValue = log.getAcceptedValue();
            }
        }
        logInfo("CONFIG_ANALYSIS", "Instance " + instance + " ballot " + configBallot + ": " + respondingAcceptors + "/" + configAcceptors.size() + " acceptors, votes=" + valueCount);
        
        // INSUFFICIENT_ACCEPTORS - Not enough acceptors for reliable decision
        if (respondingAcceptors < quorum) {
            logInfo("CONFIG_ANALYSIS", "INSUFFICIENT_ACCEPTORS: need " + quorum + " but only " + respondingAcceptors + " responded");
            String reason = "VERTICAL_PAXOS INSUFFICIENT_ACCEPTORS in ballot " + configBallot + " (" + respondingAcceptors + "/" + quorum + " acceptors responded)";
            return new RecoveryAction(instance, highestBallotValue > 0 ? highestBallotValue : -1, false, reason);
        }
        // CONSENSUS - A value has quorum agreement
        for (java.util.Map.Entry<Integer, Integer> entry : valueCount.entrySet()) {
            if (entry.getValue() >= quorum) {
                logInfo("CONFIG_ANALYSIS", "CONSENSUS: value " + entry.getKey() + " has " + entry.getValue() + "/" + quorum + " votes");
                String reason = "VERTICAL_PAXOS CONSENSUS in ballot " + configBallot + " (" + entry.getValue() + "/" + quorum + " needed from " + configAcceptors.size() + " acceptors) - value " + entry.getKey();
                return new RecoveryAction(instance, entry.getKey(), true, reason);
            }
        }
        // NO_CONSENSUS - Enough acceptors but they disagree
        if (highestBallotValue != -1) {
            logInfo("CONFIG_ANALYSIS", "NO_CONSENSUS: using highest ballot " + highestBallot + " value " + highestBallotValue);
            String reason = "VERTICAL_PAXOS NO_CONSENSUS in ballot " + configBallot + " - highest ballot " + highestBallot + ", value " + highestBallotValue;
            return new RecoveryAction(instance, highestBallotValue, false, reason);
        }
        return null;
    }    
    /**
     * Helper method to limit ballot number for debug purposes
     */
    private int limitBallot(int ballot) {
        if (ballot > 5) {
            return 5;
        }
        return ballot;
    }
    
    /**
     * Helper method to extract ballot number from reason string (for comparison)
     */
    private int extractBallotFromReason(String reason) {
        // Simple extraction - look for "ballot X" pattern
        try {
            int ballotIndex = reason.indexOf("ballot ");
            if (ballotIndex != -1) {
                String substr = reason.substring(ballotIndex + 7);
                int spaceIndex = substr.indexOf(" ");
                if (spaceIndex != -1) {
                    return Integer.parseInt(substr.substring(0, spaceIndex));
                } else {
                    return Integer.parseInt(substr);
                }
            }
        } catch (NumberFormatException e) {
            // Ignore parsing errors
        }
        return -1;
    }

    /**
     * Determines the maximum instance to analyze during log recovery.
     * Uses ONLY acceptor logs to determine distributed instances that need recovery.
     */
    private int determineMaxInstanceToAnalyze(java.util.Map<Integer, java.util.List<didameetings.DidaMeetingsPaxos.InstanceLog>> instanceMap) {
        int receivedMaxInstance = 0;
        
        // Find the highest instance number in received logs (distributed instances only)
        for (Integer instance : instanceMap.keySet()) {
            if (instance >= receivedMaxInstance) {
                receivedMaxInstance = instance + 1;
            }
        }
        
        // Only analyze distributed instances (ignore local-only instances)
        int maxInstance = receivedMaxInstance;
        
        logInfo("RECOVERY", "Instances in collected logs: " + instanceMap.keySet());
        
        return maxInstance;
    }

	// Storage for complete logs from ALL servers (organized by server ID)
	private volatile java.util.Map<Integer, java.util.List<didameetings.DidaMeetingsPaxos.InstanceLog>> 
		completeLogsByServerId = new java.util.concurrent.ConcurrentHashMap<>();
	
	// Fast Paxos Manager for TOPIC operations
	private final fastTopicManager fastTopicManager = new fastTopicManager(this);
	
	/**
	 * Get the Fast Paxos manager (for external access)
	 */
	public fastTopicManager getfastTopicManager() {
		return fastTopicManager;
	}
	
	/** Store complete logs from ALL servers, organized by server ID */
	public void storeCompleteLogsByServerId(java.util.Map<Integer, java.util.List<didameetings.DidaMeetingsPaxos.InstanceLog>> logsByServerId) {
		this.completeLogsByServerId.clear();
		this.completeLogsByServerId.putAll(logsByServerId);
		
		int totalEntries = logsByServerId.values().stream().mapToInt(java.util.List::size).sum();
		logInfo("LOG_STORAGE", "Stored complete logs from " + logsByServerId.size() + " servers (" + totalEntries + " total entries) - organized by server ID");
		
		// Debug: Show which servers we have logs from
		for (java.util.Map.Entry<Integer, java.util.List<didameetings.DidaMeetingsPaxos.InstanceLog>> entry : logsByServerId.entrySet()) {
			logInfo("LOG_STORAGE", "Server " + entry.getKey() + ": " + entry.getValue().size() + " log entries");
		}
	}
	
	/** Get logs from specific servers (for config-specific recursive analysis) */
	public java.util.Map<Integer, java.util.List<didameetings.DidaMeetingsPaxos.InstanceLog>> 
		getLogsFromServers(java.util.List<Integer> serverIds) {
		
		java.util.Map<Integer, java.util.List<didameetings.DidaMeetingsPaxos.InstanceLog>> result = 
			new java.util.HashMap<>();
			
		for (Integer serverId : serverIds) {
			if (completeLogsByServerId.containsKey(serverId)) {
				result.put(serverId, completeLogsByServerId.get(serverId));
			}
		}
		if (result.isEmpty()) {
			logInfo("LOG_THREAD", "Background collection not complete yet - will use Phase 1 logs (servers: " + serverIds + ")");
		} else {
			logInfo("LOG_THREAD", "Retrieved COMPLETE logs from " + result.size() + " out of " + serverIds.size() + " requested servers: " + serverIds);
		}
		return result;
	}

    /**
     * FAST PAXOS: Processes TOPIC operations using Fast Paxos protocol with logical timestamp synchronization.
     * Sends proposals directly to all acceptors, bypassing leader.
     * Returns true if consensus achieved
     */
    public boolean processfastTopicRequest(RequestRecord request_record, int clientTimestamp) {
        // Delegate to the Fast Paxos manager with timestamp synchronization
        return fastTopicManager.processfastTopicRequest(request_record, clientTimestamp);
    }
    
    /** Notify Fast Paxos manager about ADD requests for predictive execution */
    public void notifyAddRequest(int meetingId, int participantId) {
        fastTopicManager.notifyAddRequest(meetingId, participantId);
    }

	@Override
	public String toString() {
		return "DidaMeetingsServerState{" +
				"my_id=" + my_id +
				", max_participants=" + max_participants +
				", current_ballot=" + current_ballot +
				", freezed=" + freezed +
				", slow_mode=" + slow_mode +
				", lastDelivered=" + lastDelivered.get() +
				'}';
	}
	
	// fastTopicInstanceManager removed - using simple counter in fastTopicManager instead
}
