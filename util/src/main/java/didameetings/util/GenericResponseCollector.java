package didameetings.util;

import java.util.ArrayList;

public class GenericResponseCollector<T>  {
    private GenericResponseProcessor     processor;
    private ArrayList<T>                 collected_responses;
    private int                          received;
    private int                          pending;
    private boolean                      done;
    
    // Configuration-aware quorum tracking
    private java.util.List<Integer>      previousConfigServers;
    private int                          previousConfigQuorum;
    private int                          previousConfigResponses;
    private boolean                      previousConfigQuorumReached;

    public GenericResponseCollector(ArrayList<T> responses, int maxresponses) {
	this.processor           = null;
        this.collected_responses = responses;
	this.received            = 0;
	this.pending             = maxresponses;
	this.done                = false;
    }

    public GenericResponseCollector(ArrayList<T> responses, int maxresponses, GenericResponseProcessor p) {
	this.processor           = p;
        this.collected_responses = responses;
	this.received            = 0;
	this.pending             = maxresponses;
	this.done                = false;
	// No config tracking for legacy constructor
	this.previousConfigServers = null;
	this.previousConfigQuorum = 0;
	this.previousConfigResponses = 0;
	this.previousConfigQuorumReached = false;
    }
    
    // NEW: Constructor with previous config tracking
    public GenericResponseCollector(ArrayList<T> responses, int maxresponses, GenericResponseProcessor p,
                                   java.util.List<Integer> prevConfigServers, int prevConfigQuorum) {
	this.processor           = p;
        this.collected_responses = responses;
	this.received            = 0;
	this.pending             = maxresponses;
	this.done                = false;
	// Config tracking
	this.previousConfigServers = new java.util.ArrayList<>(prevConfigServers);
	this.previousConfigQuorum = prevConfigQuorum;
	this.previousConfigResponses = 0;
	this.previousConfigQuorumReached = false;
    }

    public synchronized void addResponse(T resp) {
	if (!this.done) {
	    collected_responses.add(resp);
	    if (this.processor != null) {
	        // For background collection, don't let processor stop early
	        // Only use processor result for legacy behavior when no config tracking
	        if (previousConfigServers == null) {
	            this.done = this.processor.onNext (this.collected_responses, resp);
	        } else {
	            // Just process the response, but don't stop collection
	            this.processor.onNext (this.collected_responses, resp);
	        }
	    }
	}
	
	// Track previous config responses if configured
	if (previousConfigServers != null && resp instanceof didameetings.DidaMeetingsPaxos.PhaseOneReply) {
	    didameetings.DidaMeetingsPaxos.PhaseOneReply phaseOneReply = 
	        (didameetings.DidaMeetingsPaxos.PhaseOneReply) resp;
	    
	    if (previousConfigServers.contains(phaseOneReply.getServerid())) {
	        previousConfigResponses++;
	        if (previousConfigResponses >= previousConfigQuorum && !previousConfigQuorumReached) {
	            previousConfigQuorumReached = true;
				System.out.println("[ACCEPTORS_QUORUM] Previous config quorum REACHED - " + previousConfigResponses + "/" + previousConfigQuorum + " from servers " + previousConfigServers);
	        }
	    }
	}
	
	this.received++;
	this.pending--;
	if (this.pending==0)
	    this.done = true;
	notifyAll();
    }

    public synchronized void addNoResponse() {
	this.pending--;
	if (this.pending==0)
	    this.done = true;
	notifyAll();
    }
 
    public synchronized void waitForQuorum(int quorum) {
        while ((this.done == false) && (this.received < quorum)) {
            try {
		wait ();
	    }
	    catch (InterruptedException e) {
	    }
	}
	// DO NOT set this.done = true here! Let addResponse() or waitUntilDone() handle completion
    }
    
    // NEW: Wait specifically for previous config quorum 
    public synchronized void waitForPreviousConfigQuorum() {
        while (!this.previousConfigQuorumReached && !this.done) {
            try {
		wait ();
	    }
	    catch (InterruptedException e) {
	    }
	}
    }
    
    // NEW: Check if previous config quorum reached
    public synchronized boolean hasPreviousConfigQuorum() {
        return this.previousConfigQuorumReached;
    }

    // Accelerated collection helpers
    public synchronized int getResponseCount() { return this.received; }
    public synchronized boolean isDone() { return this.done; }
    
    public synchronized void waitUntilDone() {
        while (this.done == false) {
            try {
		wait ();
	    }
	    catch (InterruptedException e) {
	    }
	}
    }
}
