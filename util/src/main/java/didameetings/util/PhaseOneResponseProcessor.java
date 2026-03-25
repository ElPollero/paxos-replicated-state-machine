package didameetings.util;

import didameetings.DidaMeetingsPaxos.PhaseOneReply;
import didameetings.DidaMeetingsPaxos.InstanceLog;
import didameetings.configs.ConfigurationScheduler;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class PhaseOneResponseProcessor extends GenericResponseProcessor<PhaseOneReply> {
    private final int quorum;
    private final ConfigurationScheduler scheduler;
    private final int lowBallot;
    private final int highBallot;

    private final AtomicInteger oks = new AtomicInteger(0);
    private final AtomicInteger nacks = new AtomicInteger(0);
    private final AtomicInteger maxBallotSeen = new AtomicInteger(-1);
    private final AtomicReference<PhaseOneReply> best = new AtomicReference<>(null);
    
    // Log collection for recovery
    private final Map<Integer, List<InstanceLog>> logsByAcceptor = new HashMap<>();
    private final java.util.Set<Integer> respondingServers = new java.util.HashSet<>();

    public PhaseOneResponseProcessor(ConfigurationScheduler scheduler, int lowBallot, int highBallot) {
        this.scheduler = scheduler;
        this.lowBallot = lowBallot;
        this.highBallot = highBallot;
        this.quorum = scheduler.quorum(highBallot);
    }


    @Override
    public boolean onNext(ArrayList<PhaseOneReply> allResponses, PhaseOneReply r) {
        maxBallotSeen.accumulateAndGet(r.getMaxballot(), Math::max);
        
        // Track all responding servers (even if no logs)
        synchronized (respondingServers) {
            respondingServers.add(r.getServerid());
        }
        
        // Collect logs for recovery processing
        if (r.getLogsCount() > 0) {
            synchronized (logsByAcceptor) {
                logsByAcceptor.put(r.getServerid(), new ArrayList<>(r.getLogsList()));
            }
        }
        
        if (r.getAccepted()) {
            oks.incrementAndGet();
            PhaseOneReply prev = best.get();
            if (prev == null || r.getValballot() > prev.getValballot()) {
                best.set(r);
            }
        } else {
            nacks.incrementAndGet();
        }
        return oks.get() >= quorum || nacks.get() > ((quorum * 2 - 1) - oks.get());
    }

    public boolean hasQuorumOk() {
        return oks.get() >= quorum;
    }

    public boolean hasPreviousQuorum(int previousQuorum) {
        return oks.get() + nacks.get() >= previousQuorum;
    }

    public boolean getAccepted() {
        PhaseOneReply bestReply = best.get();
        return bestReply != null && bestReply.getAccepted();
    }

    public int getMaxballot() {
        return maxBallotSeen.get();
    }

    public int getValballot() {
        PhaseOneReply bestReply = best.get();
        return bestReply != null ? bestReply.getValballot() : -1;
    }

    public int getValue() {
        PhaseOneReply bestReply = best.get();
        return bestReply != null ? bestReply.getValue() : -1;
    }

    public int getMaxBallotSeen() {
        return maxBallotSeen.get();
    }

    public PhaseOneReply bestAccepted() {
        return best.get();
    }
    
    /**
     * Returns the collected logs from all acceptors for recovery processing
     */
    public Map<Integer, List<InstanceLog>> getCollectedLogs() {
        synchronized (logsByAcceptor) {
            return new HashMap<>(logsByAcceptor);
        }
    }
    
    /**
     * Returns true if any logs were collected from acceptors
     */
    public boolean hasCollectedLogs() {
        synchronized (logsByAcceptor) {
            return !logsByAcceptor.isEmpty();
        }
    }
    
    /**
     * Returns the set of all servers that responded (with or without logs)
     */
    public java.util.Set<Integer> getRespondingServers() {
        synchronized (respondingServers) {
            return new java.util.HashSet<>(respondingServers);
        }
    }
}
