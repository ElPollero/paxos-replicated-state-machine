
package didameetings.app;


import java.util.*;

import didameetings.core.Meeting;
import didameetings.core.Participant;

import didameetings.DidaMeetingsMain;
import didameetings.DidaMeetingsMainServiceGrpc;

import didameetings.util.GenericResponseCollector;
import didameetings.util.CollectorStreamObserver;

import didameetings.configs.*;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;


public class DidaMeetingsApp {
    public static final int     MAX_PARTICIPANTS = 10;
    private boolean interactive_mode;
    private int meetings_range;
    private int participant_range;
    private int topic_range;
    private int sleep_range;
    private int loop_size;
    private Random rnd;
    private Hashtable<Integer, Meeting> open_meetings;
    private Hashtable<Integer, Meeting> closed_meetings;
    private String host;
    private int port;
    private int n_servers;
    private int client_id;
    private int sequence_number;
    private int responses_needed;
    private String[] targets;
    private ManagedChannel[] channels;
    private DidaMeetingsMainServiceGrpc.DidaMeetingsMainServiceStub[] async_stubs;
    private char schedule;
    private ConfigurationScheduler scheduler;
    private int latest_logical_timestamp;
    
    
    public DidaMeetingsApp () {
	this.interactive_mode = true;
	this.meetings_range = 1000;
	this.topic_range = 1000;
	this.participant_range = 1000;
	this.sleep_range = 6;
	this.loop_size = 20;
	this.rnd = new Random();
	this.n_servers = 6;
	this.client_id = 1;
	this.port = 8080;
	this.host = "localhost";
	this.sequence_number = 0;
	this.responses_needed = 1;
	this.rnd = new Random();
	this.targets = new String[n_servers];
	this.open_meetings = new Hashtable<Integer, Meeting>();
	this.closed_meetings = new Hashtable<Integer, Meeting>();
	this.schedule = 'A';
	this.scheduler = null;
	this.latest_logical_timestamp = 0;
    }


    private boolean open (int mid) {
	this.sequence_number = this.sequence_number+1;
	int reqid = this.sequence_number*100 + client_id;
	boolean result = false;
	
	DidaMeetingsMain.OpenRequest.Builder open_request = DidaMeetingsMain.OpenRequest.newBuilder();
	open_request.setReqid(reqid);
	open_request.setMeetingid(mid);

	
	ArrayList<DidaMeetingsMain.OpenReply> open_responses = new ArrayList<DidaMeetingsMain.OpenReply>();
	GenericResponseCollector<DidaMeetingsMain.OpenReply> open_collector = new GenericResponseCollector<DidaMeetingsMain.OpenReply> (open_responses, n_servers);
	
	for (int i = 0; i < n_servers; i++) {
	    CollectorStreamObserver<DidaMeetingsMain.OpenReply> open_observer = new CollectorStreamObserver<DidaMeetingsMain.OpenReply>(open_collector);
	    async_stubs[i].open(open_request.build(), open_observer);
	}
	open_collector.waitForQuorum(responses_needed);
	if (open_responses.size() >= responses_needed) {
	    // Synchronized access to prevent ConcurrentModificationException
	    DidaMeetingsMain.OpenReply open_reply;
	    synchronized (open_responses) {
	        if (!open_responses.isEmpty()) {
	            open_reply = open_responses.get(0);
	        } else {
	            result = false;
	            System.out.println("Open meeting operation failed - no responses\n");
	            return result;
	        }
	    }
	    result = open_reply.getResult();
	    // Update latest logical timestamp from Multi-Paxos response
	    int response_timestamp = open_reply.getLogicalTimestamp();
	    if (response_timestamp > 0) {
	        this.latest_logical_timestamp = Math.max(this.latest_logical_timestamp, response_timestamp);
	    }
	    if (result) {
		System.out.println("Meeting opened with id " + mid + "\n");
	    } else {
		System.out.println("Open meeting operation failed\n");
	    }
	}
	else
	    System.out.println("Panic...error opening a meeting\n");

	return result;
    }

    private boolean add (int mid, int pid) {
	this.sequence_number = this.sequence_number+1;
	int reqid = this.sequence_number*100 + client_id;
	boolean result = false;

	// System.out.println("Reqid " + reqid);
	// System.out.println("Meeting id " + mid + " participant id " + pid);
	
	DidaMeetingsMain.AddRequest.Builder add_request = DidaMeetingsMain.AddRequest.newBuilder();
	add_request.setReqid(reqid);
	add_request.setMeetingid(mid);
	add_request.setParticipantid(pid);

	ArrayList<DidaMeetingsMain.AddReply> add_responses = new ArrayList<DidaMeetingsMain.AddReply>();
	GenericResponseCollector<DidaMeetingsMain.AddReply> add_collector = new GenericResponseCollector<DidaMeetingsMain.AddReply> (add_responses, n_servers);
	
	for (int i = 0; i < n_servers; i++) {
	    CollectorStreamObserver<DidaMeetingsMain.AddReply> add_observer = new CollectorStreamObserver<DidaMeetingsMain.AddReply>(add_collector);
	    async_stubs[i].add(add_request.build(), add_observer);
	}
	add_collector.waitForQuorum(responses_needed);
	if (add_responses.size() >= responses_needed) {
	    // Synchronized access to prevent ConcurrentModificationException
	    DidaMeetingsMain.AddReply add_reply;
	    synchronized (add_responses) {
	        if (!add_responses.isEmpty()) {
	            add_reply = add_responses.get(0);
	        } else {
	            result = false;
	            System.out.println("Add participant to meeting operation failed - no responses\n");
	            return result;
	        }
	    }
	    result = add_reply.getResult();
	    // Update latest logical timestamp from Multi-Paxos response
	    int response_timestamp = add_reply.getLogicalTimestamp();
	    if (response_timestamp > 0) {
	        this.latest_logical_timestamp = Math.max(this.latest_logical_timestamp, response_timestamp);
	    }
	    if (result) {
		System.out.println("Added participant with id " + pid + " to meeting with id " + mid + "\n");
	    } else {
		System.out.println("Add participant to meeting operation failed\n");
	    }
	}
	else
	    System.out.println("Panic...error adding a participant to a meeting\n");

	return result;
    }

    private boolean topic (int mid, int pid, int topic) {
	this.sequence_number = this.sequence_number+1;
	int reqid = this.sequence_number*100 + client_id;
	boolean result = false;
	
	DidaMeetingsMain.TopicRequest.Builder topic_request = DidaMeetingsMain.TopicRequest.newBuilder();
	topic_request.setReqid(reqid);
	topic_request.setMeetingid(mid);
	topic_request.setParticipantid(pid);
	topic_request.setTopicid(topic);
	topic_request.setClientTimestamp(this.latest_logical_timestamp);

	
	ArrayList<DidaMeetingsMain.TopicReply> topic_responses = new ArrayList<DidaMeetingsMain.TopicReply>();
	GenericResponseCollector<DidaMeetingsMain.TopicReply> topic_collector = new GenericResponseCollector<DidaMeetingsMain.TopicReply> (topic_responses, n_servers);
	
	for (int i = 0; i < n_servers; i++) {
	    CollectorStreamObserver<DidaMeetingsMain.TopicReply> topic_observer = new CollectorStreamObserver<DidaMeetingsMain.TopicReply>(topic_collector);
	    async_stubs[i].topic(topic_request.build(), topic_observer);
	}
	topic_collector.waitForQuorum(responses_needed);
	if (topic_responses.size() >= responses_needed) {
	    // Synchronized access to prevent ConcurrentModificationException
	    DidaMeetingsMain.TopicReply topic_reply;
	    synchronized (topic_responses) {
	        if (!topic_responses.isEmpty()) {
	            topic_reply = topic_responses.get(0);
	        } else {
	            result = false;
	            System.out.println("Topic operation failed - no responses\n");
	            return result;
	        }
	    }
	    result = topic_reply.getResult();
	    if (result) {
		System.out.println("Meeting with id " + mid + " participant with id " + pid + " and topic " + topic + "\n");
	    } else {
		System.out.println("Topic operation failed\n");
	    }
	}
	else
	    System.out.println("Panic...error while adding topic to a participant in a meeting\n");

	return result;
    }

    private boolean close (int mid) {
	this.sequence_number = this.sequence_number+1;
	int reqid = this.sequence_number*100 + client_id;
	boolean result = false;

	// System.out.println("Reqid " + reqid);
	// System.out.println("Meeting id " + mid);
	
	DidaMeetingsMain.CloseRequest.Builder close_request = DidaMeetingsMain.CloseRequest.newBuilder();
	close_request.setReqid(reqid);
	close_request.setMeetingid(mid);

	ArrayList<DidaMeetingsMain.CloseReply> close_responses = new ArrayList<DidaMeetingsMain.CloseReply>();
	GenericResponseCollector<DidaMeetingsMain.CloseReply> close_collector = new GenericResponseCollector<DidaMeetingsMain.CloseReply> (close_responses, n_servers);
	
	for (int i = 0; i < n_servers; i++) {
	    CollectorStreamObserver<DidaMeetingsMain.CloseReply> close_observer = new CollectorStreamObserver<DidaMeetingsMain.CloseReply>(close_collector);
	    async_stubs[i].close(close_request.build(), close_observer);
	}
	close_collector.waitForQuorum(responses_needed);
	if (close_responses.size() >= responses_needed) {
	    // Synchronized access to prevent ConcurrentModificationException
	    DidaMeetingsMain.CloseReply close_reply;
	    synchronized (close_responses) {
	        if (!close_responses.isEmpty()) {
	            close_reply = close_responses.get(0);
	        } else {
	            result = false;
	            System.out.println("Close meeting operation failed - no responses\n");
	            return result;
	        }
	    }
	    result = close_reply.getResult();
	    if (result) {
		System.out.println("Meeting closed with id " + mid + "\n");
	    } else {
		System.out.println("Close meeting operation failed\n");
	    }
	}
	else
	    System.out.println("Panic...error closing a meeting\n");

	return result;
    }

    private boolean show () {
	this.sequence_number = this.sequence_number+1;
	int reqid = this.sequence_number*100 + client_id;
	boolean result = false;

	// System.out.println("Reqid " + reqid);
	
	DidaMeetingsMain.DumpRequest.Builder dump_request = DidaMeetingsMain.DumpRequest.newBuilder();
	dump_request.setReqid(reqid);
	
	ArrayList<DidaMeetingsMain.DumpReply> dump_responses = new ArrayList<DidaMeetingsMain.DumpReply>();
	GenericResponseCollector<DidaMeetingsMain.DumpReply> dump_collector = new GenericResponseCollector<DidaMeetingsMain.DumpReply> (dump_responses, n_servers);
	
	for (int i = 0; i < n_servers; i++) {
	    CollectorStreamObserver<DidaMeetingsMain.DumpReply> dump_observer = new CollectorStreamObserver<DidaMeetingsMain.DumpReply>(dump_collector);
	    async_stubs[i].dump(dump_request.build(), dump_observer);
	}
	dump_collector.waitForQuorum(responses_needed);
	if (dump_responses.size() >= responses_needed) {
	    // Synchronized access to prevent ConcurrentModificationException
	    DidaMeetingsMain.DumpReply dump_reply;
	    synchronized (dump_responses) {
	        if (!dump_responses.isEmpty()) {
	            dump_reply = dump_responses.get(0);
	        } else {
	            result = false;
	            System.out.println("Dump failed - no responses\n");
	            return result;
	        }
	    }
	    result = dump_reply.getResult();
	    if (result) {
		System.out.println("Dump requested\n");
	    } else {
		System.out.println("Dump failed\n");
	    }
	}
	else
	    System.out.println("Panic...error on dump request\n");
	
	return result;
   }

    private void doStuff() {
	final int OPEN_THRESHOLD = 5;
	final int OPEN_TARGET = 10;
	final int ACTION_PROBABILITY_RANGE = 4;
	open_meetings = new Hashtable<Integer, Meeting>();

	int counter = 0;

	while (counter < loop_size) {
	    int n_open   = open_meetings.size();
	    int n_closed = closed_meetings.size();

	    if (n_open < OPEN_THRESHOLD) {
		int mid = rnd.nextInt(meetings_range);
		// System.out.println("open " + mid + "\n");
		if (this.open (mid)) {
		    Meeting new_meeting = new Meeting (mid, this.MAX_PARTICIPANTS);
		    open_meetings.put (mid, new_meeting);
		}
	    }
	    else {
		int action = rnd.nextInt(ACTION_PROBABILITY_RANGE);
		if (action < (ACTION_PROBABILITY_RANGE*0.25)) {
		    // open and close meetings
		    action = rnd.nextInt(ACTION_PROBABILITY_RANGE);
		    if (((n_open<OPEN_TARGET) && (action >= (ACTION_PROBABILITY_RANGE*0.25))) || ((n_open >= OPEN_TARGET) && (action < (ACTION_PROBABILITY_RANGE*0.25)))){
			int mid = rnd.nextInt(meetings_range);

			// System.out.println("open " + mid + "\n");
			this.open (mid);
		    }
		    else if (n_open > 0) {
			int selection = rnd.nextInt(n_open);
			Enumeration<Integer> ids = this.open_meetings.keys();
			int mid = 0;
			
			while (ids.hasMoreElements() && (selection >=0)) {
			    mid = ids.nextElement();
			    selection--;
			}

			// System.out.println("close " + mid + "\n");
			if(this.close(mid)) {
			    Meeting meeting = this.open_meetings.get(mid);
			    open_meetings.remove (mid);
			}
		    }
		}
		else if (action < (ACTION_PROBABILITY_RANGE*0.50)) {
		    // add topics to participants
		    Meeting meeting = null;
		    int mid = 0;
		    
		    int open_or_close =  rnd.nextInt(2);
		    if (open_or_close == 0) {
			int selection = rnd.nextInt(n_open);
			Enumeration<Integer> ids = this.open_meetings.keys();
			
			while (ids.hasMoreElements() && (selection >=0)) {
			    mid = ids.nextElement();
			    selection--;
			}
			meeting = this.open_meetings.get(mid);
		    }
		    else {
			int selection = rnd.nextInt(n_closed);
			Enumeration<Integer> ids = this.closed_meetings.keys();
			
			while (ids.hasMoreElements() && (selection >=0)) {
			    mid = ids.nextElement();
			    selection--;
			}
			meeting = this.closed_meetings.get(mid);
		    }
		    if (meeting != null) {
			Enumeration<Integer> participants = meeting.participantsWithoutTopic();
			int n_candidates = meeting.numberOfParticipantsWithoutTopic();
			if (n_candidates > 0) {
			    int selection = rnd.nextInt(n_candidates);
			    int pid = 0 ;
			    while (participants.hasMoreElements() && (selection >=0)) {
				pid = participants.nextElement();
				selection--;
			    }
			    int topic = rnd.nextInt(this.topic_range);
			    meeting.setTopic(pid, topic);
			    topic (mid, pid, topic);
			}
		    }
		}
		else if (n_open > 0) {
		    // add participants to meetings
		    int selection = rnd.nextInt(n_open);
		    Enumeration<Integer> ids = this.open_meetings.keys();
		    int mid = 0;
		    
		    while (ids.hasMoreElements() && (selection >=0)) {
			mid = ids.nextElement();
			selection--;
		    }
		    int pid = rnd.nextInt(participant_range);

		    // System.out.println("add participant " + pid + " to meetings " + mid + "\n");
		    this.add (mid, pid);
		}
	    }
            counter++;
        }
    }
	

    public void parseArgs (String[] args) {
	int length = args.length;
	int cursor = 0;

	cursor = 1;
	while (cursor < length) {
	    String option = args[cursor];
	    String[] option_parts = option.split(" ");
            String option_name = option_parts[0].toLowerCase();
            String option_parameter = option_parts.length > 1 ? option_parts[1] : null;

	    switch (option_name) {
	        case "--help":
                    System.out.printf("\n--help");
		    System.out.printf("\n--mrange meetingrange");  
		    System.out.printf("\n--prange participantrange");  
		    System.out.printf("\n--trange topicrange");  
		    System.out.printf("\n--lenght looplenght");  
  		    System.out.printf("\n--sleep sleeprange");  
 		    System.out.printf("\n-i (iterative mode)\n");
		    cursor++;
		    break;
		case "--mrange":
		    if (option_parameter==null)
			System.err.println("missing meetingrange");
		    else 
			meetings_range = Integer.parseInt(option_parameter);
		    break;
		case "--trange":
		    if (option_parameter==null)
			System.err.println("missing topicrange");
		    else 
			topic_range = Integer.parseInt(option_parameter);
		    break;
		case "--prange":
		    if (option_parameter==null)
			System.err.println("missing participantrange");
		    else 
			participant_range = Integer.parseInt(option_parameter);
		    break;
		case "--lenght":
		    if (option_parameter==null)
			System.err.println("missing looplenght");
		    else 
			loop_size = Integer.parseInt(option_parameter);
		    break;
		case "--sleep":
		    if (option_parameter==null)
			System.err.println("missing sleeprange");
		    else 
			sleep_range= Integer.parseInt(option_parameter);
		    break;
		case "-i":
		    interactive_mode = true;
		    break;
	        default:
		    System.err.println("Unknown option");
		    break;
	     }
	    cursor++;
	}
    }

    public void goInteractive() {
	Scanner scanner = new Scanner(System.in);
        String command;

	boolean keep_going = true;
        
        while (keep_going) {
            System.out.print("app> ");
            command = scanner.nextLine();
            String[] commandParts = command.split(" ");
            String mainCommand = commandParts[0].toLowerCase();
            String parameter1 = commandParts.length > 1 ? commandParts[1] : null;
            String parameter2 = commandParts.length > 2 ? commandParts[2] : null;
            String parameter3 = commandParts.length > 3 ? commandParts[3] : null;

            switch (mainCommand) {
	        case "help":
                    System.out.println("\thelp");
                    System.out.println("\topen meeting_id");
		    System.out.println("\tclose meeting_id");
		    System.out.println("\tadd meeting_id participant_id");
		    System.out.println("\ttopic meeting_id participant_id topic_id");
		    System.out.println("\tshow");
		    System.out.println("\tloop");
		    System.out.println("\tmrange meeting-range");
		    System.out.println("\tprange participant-range");
		    System.out.println("\ttrange topic-range");
		    System.out.println("\tlenght loop-lenght");
		    System.out.println("\ttime sleep-range");
		    System.out.println("\texit");
                    break;
                case "open":
		    System.out.println("open " + parameter1);
                    if (parameter1 != null) {
			try {
			    int mid =  Integer.parseInt(parameter1);
			    if (open(mid))
				System.out.println("meeting with id " + mid + " is open.");
			    else
				System.out.println("failed to open " + mid);
			} catch (NumberFormatException e) {
			    System.out.println("usage: open meeting_id");
		        }
		    } else 
			System.out.println("usage: open meeting_id");
                    break;
                case "close":
		    System.out.println("close " + parameter1);
                    if (parameter1 != null) {
			try {
			    int mid =  Integer.parseInt(parameter1);
			    if (close(mid))
				System.out.println("meeting with id " + mid + " is closed.");
			    else
				System.out.println("failed to close " + mid);
			} catch (NumberFormatException e) {
			    System.out.println("usage: close meeting_id");
		        }
		    } else 
			System.out.println("usage: close meeting_id");
                    break;
               case "topic":
		    System.out.println("topic " + parameter1 + " " + parameter2 + " " + parameter3);
                    if (parameter1 != null && parameter2 != null && parameter3 != null) {
			try {
			    int mid = Integer.parseInt(parameter1);
			    int pid = Integer.parseInt(parameter2);
			    int tid = Integer.parseInt(parameter3);
			    if (topic(mid, pid, tid))
				System.out.println("added topic " + tid + " to participant " + pid + " in meeting  " + mid);
			    else
				System.out.println("failed to add topic " + tid + " to participant " + pid + " in meeting " + mid);
			} catch (NumberFormatException e) {
			    System.out.println("usage: topic meeting_id participant_id topic_id");
		        }
		    } else 
			System.out.println("usage: lock meeting_idusage: topic meeting_id participant_id topic_id");
                    break;
              case "add":
		    System.out.println("add " + parameter1 + " " + parameter2);
                    if (parameter1 != null && parameter2 != null) {
			try {
			    int mid =  Integer.parseInt(parameter1);
			    int pid =  Integer.parseInt(parameter2);
			    if (add(mid,pid))
				System.out.println("participant " + pid + " added to meeting with id " + mid + ".");
			    else
				System.out.println("failed to add  participant " + pid + " to meeting with id " + mid + ".");
			} catch (NumberFormatException e) {
			    System.out.println("usage: add meeting_id participant_id");
		        }
		    } else 
			System.out.println("usage: add meeting_id participant_id");
                    break;
               case "show":
		     try {
			show();
		     } catch (Exception e) {
		     }
		    break;
 	        case "lenght":
		    System.out.println("lenght " + parameter1);
                    if (parameter1 != null) {
			try {
			     loop_size=  Integer.parseInt(parameter1);
			} catch (NumberFormatException e) {
			    System.out.println("usage: lenght loop-lenght");
		        }
		    } else 
			System.out.println("usage: lenght loop-lenght");
                    break;
	        case "mrange":
		    System.out.println("mrange " + parameter1);
                    if (parameter1 != null) {
			try {
			     meetings_range =  Integer.parseInt(parameter1);
			} catch (NumberFormatException e) {
			    System.out.println("usage: mrange meetings-range");
		        }
		    } else 
			System.out.println("usage: mrange meetings-range");
                    break;
	        case "prange":
		    System.out.println("prange " + parameter1);
                    if (parameter1 != null) {
			try {
			     participant_range =  Integer.parseInt(parameter1);
			} catch (NumberFormatException e) {
			    System.out.println("usage: prange participants-range");
		        }
		    } else 
			System.out.println("usage: prange participants-range");
                    break;
	        case "time":
		    System.out.println("time " + parameter1);
                    if (parameter1 != null) {
			try {
			     sleep_range=  Integer.parseInt(parameter1);
			} catch (NumberFormatException e) {
			    System.out.println("usage: time sleep-range");
		        }
		    } else 
			System.out.println("usage: time sleep-range");
                    break;
                 case "loop":
		     try {
			doStuff();
		     } catch (Exception e) {
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
    }
    
    private void initComms () {
	// Let us use plaintext communication because we do not have certificates
	this.channels = new ManagedChannel[n_servers];

	for (int i = 0; i < n_servers; i++) {
	    this.channels[i] = ManagedChannelBuilder.forTarget(targets[i]).usePlaintext().build();
	}
	
	this.async_stubs = new DidaMeetingsMainServiceGrpc.DidaMeetingsMainServiceStub[n_servers];

	for (int i = 0; i < n_servers; i++) {
	    this.async_stubs[i] = DidaMeetingsMainServiceGrpc.newStub(channels[i]);
	}
    }

    private void terminateComms () {
	for (int i = 0; i < n_servers; i++) {
	    this.channels[i].shutdownNow();
	}
    }

    public void main_loop (String[] args) throws Exception {
	System.out.println(DidaMeetingsApp.class.getSimpleName());

	
	// receive and print arguments
	System.out.printf("Received %d arguments%n", args.length);
	for (int i = 0; i < args.length; i++) {
	    System.out.printf("arg[%d] = %s%n", i, args[i]);
	}

		
	// check arguments
	if (args.length < 4) {
	    System.err.println("Argument(s) missing!");
	    System.err.printf("Usage: java %s client id host port schedule%n", DidaMeetingsApp.class.getName());
	    return;
	}

	// set client id
	this.client_id =  Integer.parseInt(args[0]);
	if ((this.client_id < 1) || (this.client_id > 99)) {
	    System.err.println("Error: client id needs to be in interval [0,99].");
	    return;
	}

	// set servers
	this.host = args[1];
        this.port = Integer.parseInt(args[2]);

	// set scheduler
	this.schedule  = args[3].charAt(0);
	this.scheduler = new ConfigurationScheduler (schedule);
	this.n_servers = scheduler.allparticipants().size();

	// check arguments
	this.parseArgs(args);

	// print parameters
	System.out.println("Client id = " + this.client_id + " serverhost = " + this.host + " port = " + this.port);
	System.out.println("Client meetings_range = " + this.meetings_range + " participants_range = " + this.participant_range + " sleep_range = " + this.sleep_range + " loop_size = " + this.loop_size);
	System.out.println("Interactive mode = " + this.interactive_mode);

	// set servers
	for (int i = 0; i < this.n_servers; i++) {
	    int target_port = this.port +i;
	    this.targets[i] = new String();
	    this.targets[i] = this.host + ":" + target_port;
	    System.out.printf("targets[%d] = %s%n", i, this.targets[i]);
	}

	// init the communication stuff
	this.initComms();

	// do work
	if (interactive_mode == false)
	    doStuff();
	else
	    goInteractive();

	// shutdown
        System.out.println("closing channels...");
	this.terminateComms();
        System.out.println("Exiting...");
	
    }
    
    public static void main(String[] args) throws Exception {
	System.out.println("Starting...");
	
	DidaMeetingsApp app = new DidaMeetingsApp();

	app.main_loop(args);
    }
}



