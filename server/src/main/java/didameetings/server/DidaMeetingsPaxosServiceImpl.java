
package didameetings.server;


import java.util.*;

import didameetings.DidaMeetingsPaxos;
import didameetings.DidaMeetingsPaxosServiceGrpc;

import io.grpc.stub.StreamObserver;
import io.grpc.Context;

public class DidaMeetingsPaxosServiceImpl extends DidaMeetingsPaxosServiceGrpc.DidaMeetingsPaxosServiceImplBase {
    DidaMeetingsServerState server_state;

    public DidaMeetingsPaxosServiceImpl(DidaMeetingsServerState state) {
	this.server_state = state;
    }
    
    // Helper for cleaner logging
    private void logInfo(String category, String message) {
        System.out.println("[" + category + "] " + message);
    }
 
    @Override
    public void phaseone(DidaMeetingsPaxos.PhaseOneRequest request, StreamObserver<DidaMeetingsPaxos.PhaseOneReply> responseObserver) {
	logInfo("PHASE1", "Receive phase1 request: " + request);
	
	// Apply slow mode delay (not for console requests)
	this.server_state.applySlowModeDelay();
	
	// Check if server is freezed
	synchronized (this.server_state) {
	    while (this.server_state.isFreezed()) {
	        try {
	            logInfo("FREEZE", "Server is frozen in phaseone, waiting for unfreeze command...");
	            this.server_state.wait();
	        } catch (InterruptedException e) {
	            Thread.currentThread().interrupt();
	        }
	    }
	}

	int instance          = request.getInstance();
	int ballot            = request.getRequestballot();
	PaxosInstance entry   = this.server_state.paxos_log.testAndSetEntry(instance, ballot);
	boolean accepted      = false;
	int  value;
	int  valballot;
	int  maxballot;
	
	synchronized (entry) {
	    if (ballot >= this.server_state.getCurrentBallot()) {
	        accepted = true;
	        this.server_state.setCurrentBallot(ballot);
	        entry.read_ballot = ballot;
	    }
	    
	    value = entry.command_id;
	    valballot = entry.write_ballot;
	    maxballot = this.server_state.getCurrentBallot();
	}

	logInfo("PHASE1", "Instance=" + instance + " ballot=" + ballot + " current_ballot=" + this.server_state.getCurrentBallot() + 
	        " val=" + value + " valballot=" + valballot + " maxballot=" + maxballot + " accepted=" + accepted);
	
	DidaMeetingsPaxos.PhaseOneReply.Builder response_builder = DidaMeetingsPaxos.PhaseOneReply.newBuilder();
	response_builder.setInstance(instance);
	response_builder.setServerid(this.server_state.my_id);
	response_builder.setRequestballot(ballot);
	response_builder.setAccepted(accepted);
	response_builder.setValue(value);
	response_builder.setValballot(valballot);
	response_builder.setMaxballot(maxballot);
	
	// Add log entries if requested for recovery  
	if (request.getRequestLogRecovery()) {
	    // Send complete logs for comprehensive recovery mechanism
	    java.util.List<didameetings.DidaMeetingsPaxos.InstanceLog> logs = this.server_state.getAllLogEntries();
	    response_builder.addAllLogs(logs);
	    logInfo("LOG_RECOVERY", "Sending " + logs.size() + " log entries to leader for recovery");
	}

	DidaMeetingsPaxos.PhaseOneReply response = response_builder.build();

	logInfo("PHASE1", "Sending phase1 response: " + response);
	
	responseObserver.onNext(response);
	responseObserver.onCompleted();
    }

    @Override
    public void phasetwo(DidaMeetingsPaxos.PhaseTwoRequest request, StreamObserver<DidaMeetingsPaxos.PhaseTwoReply> responseObserver) {
	logInfo("PHASE2", "Receive phase two request: " + request);
	// Apply slow mode delay (not for console requests)
	this.server_state.applySlowModeDelay();
	
	// Check if server is freezed
	synchronized (this.server_state) {
	    while (this.server_state.isFreezed()) {
	        try {
	            logInfo("FREEZE", "Server is frozen in phasetwo, waiting for unfreeze command...");
	            this.server_state.wait();
	        } catch (InterruptedException e) {
	            Thread.currentThread().interrupt();
	        }
	    }
	}

	int instance          = request.getInstance();
	int ballot            = request.getRequestballot();
	int value             = request.getValue();
	PaxosInstance entry   = this.server_state.paxos_log.testAndSetEntry(instance);
	boolean accepted      = false;
	int  maxballot;

	synchronized (entry) {
	    if (ballot >= this.server_state.getCurrentBallot()) {
	        accepted           = true;
	        entry.command_id   = value;
	        entry.write_ballot = ballot;
	        this.server_state.setCurrentBallot(ballot);
	        maxballot = ballot;
	    }
	    else {
	        maxballot = this.server_state.getCurrentBallot();
	    }
	}	

	DidaMeetingsPaxos.PhaseTwoReply.Builder response_builder = DidaMeetingsPaxos.PhaseTwoReply.newBuilder();
	response_builder.setAccepted(accepted);
	response_builder.setInstance(instance);
	response_builder.setServerid(this.server_state.my_id);
	response_builder.setRequestballot(ballot);
	response_builder.setMaxballot(maxballot);


	DidaMeetingsPaxos.PhaseTwoReply response = response_builder.build();
	
	logInfo("PHASE2", "Sending phase2 response: " + response);
	
	responseObserver.onNext(response);
	responseObserver.onCompleted();
	
	// Notify learners
	if (accepted == true) {
	    
	    Context ctx = Context.current().fork();
	    ctx.run(() -> {
		    List<Integer> learners = this.server_state.scheduler.learners(ballot);
		    int n_targets          = learners.size();
		    
		    DidaMeetingsPaxos.LearnRequest.Builder learn_request_builder = DidaMeetingsPaxos.LearnRequest.newBuilder();
		    learn_request_builder.setInstance(instance);
		    learn_request_builder.setValue(value);
		    learn_request_builder.setBallot(ballot);
		    
		    DidaMeetingsPaxos.LearnRequest learn_request = learn_request_builder.build();
		    
		    logInfo("LEARN", "Sending learn request: " + learn_request);
		    
		    logInfo("LEARN", "Paxos acceptor: going to notify learners for entry " + instance + " with timestamp " + ballot);
		    
		    // Fire-and-forget learn notifications
		    for (int i = 0; i < n_targets; i++) {
			io.grpc.stub.StreamObserver<DidaMeetingsPaxos.LearnReply> learn_observer = 
			    new io.grpc.stub.StreamObserver<DidaMeetingsPaxos.LearnReply>() {
				@Override
				public void onNext(DidaMeetingsPaxos.LearnReply reply) {}
				@Override  
				public void onError(Throwable t) {}
				@Override
				public void onCompleted() {}
			    };
			this.server_state.async_stubs[learners.get(i)].learn(learn_request, learn_observer);
		    }
		    logInfo("LEARN", "Learn request completed for instance = " + instance);
		});
	}
	
	
    }

    @Override
    public void learn(DidaMeetingsPaxos.LearnRequest request, StreamObserver<DidaMeetingsPaxos.LearnReply> responseObserver) {
	logInfo("LEARN", "Receive learn request: " + request);
	
	// Apply slow mode delay (not for console requests)
	this.server_state.applySlowModeDelay();
	
	// Check if server is freezed
	synchronized (this.server_state) {
	    while (this.server_state.isFreezed()) {
	        try {
	            logInfo("FREEZE", "Server is frozen in learn, waiting for unfreeze command...");
	            this.server_state.wait();
	        } catch (InterruptedException e) {
	            Thread.currentThread().interrupt();
	        }
	    }
	}

	int instance         = request.getInstance();
	int ballot           = request.getBallot();
	int value            = request.getValue();

	
	synchronized (this) {
	    PaxosInstance entry  = this.server_state.paxos_log.testAndSetEntry(instance);
	    
	    // Special handling for Fast Paxos (TOPIC commands with negative instances)
	    if (instance < 0) {
	        int currentServerBallot = this.server_state.getCurrentBallot();
	        if (ballot < currentServerBallot) {
	            logInfo("LEARN", "Fast Paxos learn: updating ballot from " + ballot + " to current " + currentServerBallot + " for TOPIC command");
	            ballot = currentServerBallot; // Update to current ballot
	        }
	    }
	    
	    logInfo("LEARN", "Paxos learner: learning entry " + instance + " with timestamp " + ballot);

	    this.server_state.setCurrentBallot(ballot);
	    
	    if (ballot == entry.accept_ballot) {
		entry.n_accepts++;
		logInfo("LEARN", "Paxos learner for instance " + instance + " : number of accepts " +  entry.n_accepts);
		if (entry.n_accepts >= this.server_state.scheduler.quorum(ballot) && !entry.decided) {
		    logInfo("LEARN", "Paxos learner: decision reached with quorum");
		    this.server_state.updateCompletedBallot(ballot);
		    
		    synchronized (entry) {
		        entry.decided = true;
		        entry.notifyAll(); // Wake up any threads waiting on this entry
		    }
		    
		    this.server_state.onDecided(instance, value);
		}
	    }
	    else if (ballot > entry.accept_ballot) {
		logInfo("LEARN", "Paxos learner for instance " + instance + " : resetting ");
		synchronized (entry) {
		    entry.command_id     = value;
		    entry.accept_ballot  = ballot;
		    entry.n_accepts      = 1;
		}
	    }
	}
	
	DidaMeetingsPaxos.LearnReply.Builder response_builder = DidaMeetingsPaxos.LearnReply.newBuilder();
	response_builder.setInstance(instance);
	response_builder.setBallot(ballot);

	DidaMeetingsPaxos.LearnReply response = response_builder.build();
	
	logInfo("LEARN", "Sending learn response");
		
	responseObserver.onNext(response);
	responseObserver.onCompleted();
    }
	
	@Override
    public void fastTopic(DidaMeetingsPaxos.fastTopicRequest request, StreamObserver<DidaMeetingsPaxos.fastTopicReply> responseObserver) {
        logInfo("FAST_TOPIC", "Receive Fast Paxos request: " + request);
        
        // Apply slow mode delay (not for console requests)
        this.server_state.applySlowModeDelay();
        
        // Check if server is frozen
        synchronized (this.server_state) {
            while (this.server_state.isFreezed()) {
                try {
                    logInfo("FREEZE", "Server is frozen in fastTopic, waiting for unfreeze command...");
                    this.server_state.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        
        int instance = request.getInstance();
        int ballot = request.getBallot();
        int value = request.getValue();
        int clientId = request.getClientId();
        
        logInfo("FAST_TOPIC", "Processing Fast Paxos: instance=" + instance + 
               ", ballot=" + ballot + ", value=" + value + ", client=" + clientId);
        
        // Get the Paxos instance
        PaxosInstance paxos_entry = this.server_state.paxos_log.testAndSetEntry(instance);
        
        boolean accepted = false;
        boolean conflict = false;
        int acceptedValue = value;
        
        synchronized (paxos_entry) {
            // Fast Paxos voting logic
            if (paxos_entry.decided) {
                // Already decided - return the decided value
                acceptedValue = paxos_entry.command_id;
                accepted = true;
                if (acceptedValue != value) {
                    conflict = true; // Different value was already decided
                }
                logInfo("FAST_TOPIC", "Instance " + instance + " already decided with value " + acceptedValue);
            } else if (paxos_entry.write_ballot == -1) {
                // No previous proposals - accept immediately (Fast Paxos fast path)
                paxos_entry.write_ballot = ballot;
                paxos_entry.command_id = value;
                accepted = true;
                logInfo("FAST_TOPIC", "Fast accept: instance=" + instance + " accepted value " + value);
            } else if (paxos_entry.write_ballot < ballot) {
                // Higher ballot - accept the new proposal
                paxos_entry.write_ballot = ballot;
                paxos_entry.command_id = value;
                accepted = true;
                logInfo("FAST_TOPIC", "Accept higher ballot: instance=" + instance + 
                       " accepted value " + value + " (ballot " + ballot + ")");
            } else if (paxos_entry.write_ballot == ballot && paxos_entry.command_id == value) {
                // Same ballot and value - accept (duplicate)
                accepted = true;
                logInfo("FAST_TOPIC", "Duplicate proposal: instance=" + instance + " already has value " + value);
            } else {
                // Conflict - different value for same ballot or lower ballot
                conflict = true;
                acceptedValue = paxos_entry.command_id;
                logInfo("FAST_TOPIC", "Conflict: instance=" + instance + 
                       " has ballot=" + paxos_entry.write_ballot + 
                       " value=" + paxos_entry.command_id + " vs proposed ballot=" + ballot + " value=" + value);
            }
        }
        
        // Build response
        DidaMeetingsPaxos.fastTopicReply.Builder responseBuilder = DidaMeetingsPaxos.fastTopicReply.newBuilder();
        responseBuilder.setInstance(instance);
        responseBuilder.setServerid(this.server_state.my_id);
        responseBuilder.setBallot(ballot);
        responseBuilder.setAccepted(accepted);
        responseBuilder.setValue(acceptedValue);
        responseBuilder.setConflict(conflict);
        
        DidaMeetingsPaxos.fastTopicReply response = responseBuilder.build();
        
        logInfo("FAST_TOPIC", "Sending Fast Paxos response: accepted=" + accepted + 
               ", conflict=" + conflict + ", value=" + acceptedValue);
        
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

}
