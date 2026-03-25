package didameetings.console;


import java.util.*;

import didameetings.DidaMeetingsMaster;
import didameetings.DidaMeetingsMasterServiceGrpc;

import didameetings.configs.*;

import didameetings.util.*;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class Console {
    private int ballot_completed = -1;
    private int last_ballot = -1;

    public synchronized void setBallotCompleted (int ballot) {
	if (ballot > this.ballot_completed)
	    this.ballot_completed = ballot;
    }
    
    public synchronized int getBallotCompleted () {
	return this.ballot_completed;
    }
    

    public synchronized int getLastBallot () {
	return this.last_ballot;
    }
    
    public synchronized void setLastBallot (int ballot) {
	if (ballot > this.last_ballot)
	    this.last_ballot = ballot;
    }
    
    public void main_loop(String[] args) throws Exception {
	int n_servers = 6;
	int replica = 0;
	int mode = 0;
	int client_id = 0;
	int sequence_number = 0;
	char schedule = 'A';
	ConfigurationScheduler scheduler = null;

	
	System.out.println(DidaMeetingsMaster.class.getSimpleName());
	
	// receive and print arguments
	System.out.printf("Received %d arguments%n", args.length);
	for (int i = 0; i < args.length; i++) {
	    System.out.printf("arg[%d] = %s%n", i, args[i]);
	}
	
	// check arguments
	if (args.length < 3) {
	    System.err.println("Argument(s) missing!");
	    System.err.printf("Usage: java %s host port schedule%n", Console.class.getName());
	    return;
	}
	

	// set servers
	String host = args[0];
        int port = Integer.parseInt(args[1]);

	schedule  = args[2].charAt(0);
	scheduler = new ConfigurationScheduler (schedule);
	n_servers = scheduler.allparticipants().size();
	
	String[] targets  = new String[n_servers];
	for (int i = 0; i < n_servers; i++) {
	    int target_port = port +i;
	    targets[i] = new String();
	    targets[i] = host + ":" + target_port;
	    System.out.printf("targets[%d] = %s%n", i, targets[i]);
	}

	// Let us use plaintext communication because we do not have certificates
	ManagedChannel[]  channels = new ManagedChannel[n_servers];
	for (int i = 0; i < n_servers; i++)
	    channels[i] = ManagedChannelBuilder.forTarget(targets[i]).usePlaintext().build();
	
	DidaMeetingsMasterServiceGrpc.DidaMeetingsMasterServiceStub[] console_async_stubs = new DidaMeetingsMasterServiceGrpc.DidaMeetingsMasterServiceStub[n_servers];
	    
	for (int i = 0; i < n_servers; i++) {
	    console_async_stubs[i] = DidaMeetingsMasterServiceGrpc.newStub(channels[i]);
	}

	
        Scanner scanner = new Scanner(System.in);
        String command;

	boolean keep_going = true;
        
        while (keep_going) {
            System.out.print("console> ");
            command = scanner.nextLine();
            String[] commandParts = command.split(" ");
            String mainCommand = commandParts[0].toLowerCase();
            String parameter1 = commandParts.length > 1 ? commandParts[1] : null;
            String parameter2 = commandParts.length > 2 ? commandParts[2] : null;
            String parameter3 = commandParts.length > 3 ? commandParts[3] : null;

            switch (mainCommand) {
	        case "help":
                    System.out.println("\thelp");
                    System.out.println("\tballot number replica");
		    System.out.println("\tdebug mode replica");
		    System.out.println("\texit");
                    break;
                case "ballot":
		    System.out.println("ballot " + parameter1 + " " + parameter2);
                    if ((parameter1 != null) && (parameter2 != null)) {
			int ballot_number  =  Integer.parseInt(parameter1);
			if (ballot_number < this.getLastBallot())
			    System.out.println("usage: ballot must be larger or equal than " + last_ballot + ".\n");
			else {
			    this.setLastBallot (ballot_number);
			    try {
				replica =  Integer.parseInt(parameter2);

				System.out.println("sending ballot " + ballot_number + " to replica " + replica + ".\n");

				sequence_number = sequence_number+1;
				int reqid = sequence_number*100 + client_id;

				DidaMeetingsMaster.NewBallotRequest.Builder newballot_request = DidaMeetingsMaster.NewBallotRequest.newBuilder();
				ArrayList<DidaMeetingsMaster.NewBallotReply> newballot_responses = new ArrayList<DidaMeetingsMaster.NewBallotReply>();; 
				GenericResponseCollector<DidaMeetingsMaster.NewBallotReply> newballot_collector = new GenericResponseCollector<DidaMeetingsMaster.NewBallotReply>(newballot_responses, 1);
				CollectorStreamObserver<DidaMeetingsMaster.NewBallotReply> newballot_observer =  new CollectorStreamObserver<DidaMeetingsMaster.NewBallotReply>(newballot_collector);
				newballot_request.setReqid(reqid);
				newballot_request.setNewballot(ballot_number);
				newballot_request.setCompletedballot(this.ballot_completed);
				console_async_stubs[replica].newballot(newballot_request.build(), newballot_observer);

				// collect the result in background
				new Thread(new Runnable() {
					public void run() {
					    newballot_collector.waitForQuorum(1);
					    if (newballot_responses.size() >= 1) {
						Iterator<DidaMeetingsMaster.NewBallotReply> newballot_iterator = newballot_responses.iterator();
						DidaMeetingsMaster.NewBallotReply newballot_reply = newballot_iterator.next();
						int completed = newballot_reply.getCompletedballot();
						System.out.println("reply received to new ballot request for ballot = " + ballot_number + " with completed = " + completed);
						setBallotCompleted (completed);
					    }
					    else
						System.out.println("no reply received to new ballot request for ballot = " + ballot_number);
					}
				    }).start();
			    } catch (NumberFormatException e) {
			    }
			}
		    } else 
			System.out.println("usage: ballot number replica");
                    break;
               case "debug":
		    System.out.println("debug " + parameter1 + " " + parameter2);
                    if ((parameter1 != null) && (parameter2 != null)) {
			try {
			    mode  =  Integer.parseInt(parameter1);
			    replica =  Integer.parseInt(parameter2);
			    System.out.println("setting debug with mode " + mode + " on replica " + replica);

			    sequence_number = sequence_number+1;
			    int reqid = sequence_number*100 + client_id;

			    DidaMeetingsMaster.SetDebugRequest.Builder setdebug_request = DidaMeetingsMaster.SetDebugRequest.newBuilder();
			    ArrayList<DidaMeetingsMaster.SetDebugReply> setdebug_responses = new ArrayList<DidaMeetingsMaster.SetDebugReply>();; 
			    GenericResponseCollector<DidaMeetingsMaster.SetDebugReply> setdebug_collector = new GenericResponseCollector<DidaMeetingsMaster.SetDebugReply>(setdebug_responses, 1);
			    CollectorStreamObserver<DidaMeetingsMaster.SetDebugReply> setdebug_observer =  new CollectorStreamObserver<DidaMeetingsMaster.SetDebugReply>(setdebug_collector);
			    setdebug_request.setReqid(reqid);
			    setdebug_request.setMode(mode);
			    console_async_stubs[replica].setdebug(setdebug_request.build(), setdebug_observer);
			    setdebug_collector.waitForQuorum(1);
			    if (setdebug_responses.size() >= 1) {
				Iterator<DidaMeetingsMaster.SetDebugReply> setdebug_iterator = setdebug_responses.iterator();
				DidaMeetingsMaster.SetDebugReply setdebug_reply = setdebug_iterator.next();
				System.out.println("reply = " + setdebug_reply.getAck());
			    }
			    else
				System.out.println("no reply received");
			} catch (NumberFormatException e) {
                           System.out.println("usage: debug mode replica");
                        }
                    } else {
                        System.out.println("usage: debug mode replica");
                    }
                    break;
                case "exit":
                    keep_going = false;
		    break;
	        case "":
		    break;
                default:
                    System.out.println("Unknown command: " + mainCommand);
                    break;
	    }
	}
	System.out.println("Exiting...");
	for (int i = 0; i < n_servers; i++) {
	    channels[i].shutdownNow();
	}
	scanner.close();
    }

      
    public static void main(String[] args) throws Exception {
	System.out.println("Starting...");
	
	Console console = new Console();

	console.main_loop(args);
    }
}



