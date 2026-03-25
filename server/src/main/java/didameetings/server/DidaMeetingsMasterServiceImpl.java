package didameetings.server;

import didameetings.DidaMeetingsMaster;
import didameetings.DidaMeetingsMasterServiceGrpc;
import io.grpc.stub.StreamObserver;

public class DidaMeetingsMasterServiceImpl extends DidaMeetingsMasterServiceGrpc.DidaMeetingsMasterServiceImplBase {
    DidaMeetingsServerState server_state;

    public DidaMeetingsMasterServiceImpl(DidaMeetingsServerState state) {
	this.server_state = state;
    }

    @Override
    public void newballot(DidaMeetingsMaster.NewBallotRequest request, StreamObserver<DidaMeetingsMaster.NewBallotReply> responseObserver) {
	System.out.println(request);

	int request_id       = request.getReqid();
	int new_ballot       = request.getNewballot();
	int completed_ballot = request.getCompletedballot();

	// for debug purposes
	System.out.println("Current ballot = " + this.server_state.getCurrentBallot() + " new ballot = " + new_ballot + " completed ballot = " + completed_ballot);

	this.server_state.setCompletedBallot (completed_ballot);
	
	if (new_ballot > this.server_state.getCurrentBallot()) {
	    this.server_state.setCurrentBallot (new_ballot);

	    // Check if this node becomes the leader for the new ballot
	    int designated_leader = this.server_state.scheduler.leader(new_ballot);
	    if (designated_leader == this.server_state.my_id) {
	        System.out.println("[LEADER] Node " + this.server_state.my_id + " is now leader for ballot " + new_ballot);
	        // The MainLoop will handle recovery on its next iteration when it runs Phase 1.
	    }

	    this.server_state.main_loop.wakeup();

	    completed_ballot = this.server_state.waitForCompletedBallot(new_ballot);
	}
	else {
	    completed_ballot = this.server_state.getCompletedBallot();
	}
	
	DidaMeetingsMaster.NewBallotReply.Builder response_builder = DidaMeetingsMaster.NewBallotReply.newBuilder();
	response_builder.setReqid(request_id);
	response_builder.setCompletedballot(completed_ballot);

	DidaMeetingsMaster.NewBallotReply response = response_builder.build();
	responseObserver.onNext(response);
       	responseObserver.onCompleted();
    }

    @Override
    public void setdebug(DidaMeetingsMaster.SetDebugRequest request, StreamObserver<DidaMeetingsMaster.SetDebugReply> responseObserver) {
	// for debug purposes
	System.out.println(request);

	boolean response_value = true;

	int request_id   = request.getReqid();
	this.server_state.setDebugMode (request.getMode());
	this.server_state.main_loop.wakeup();

	// for debug purposes
	System.out.println("Setting debug mode to = " + this.server_state.getDebugMode());

	DidaMeetingsMaster.SetDebugReply.Builder response_builder = DidaMeetingsMaster.SetDebugReply.newBuilder();
	response_builder.setReqid(request_id);
	response_builder.setAck(response_value);
	
	DidaMeetingsMaster.SetDebugReply response = response_builder.build();
	responseObserver.onNext(response);
	responseObserver.onCompleted();
    }
    
}
