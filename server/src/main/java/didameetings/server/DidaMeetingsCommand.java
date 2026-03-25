package didameetings.server;

enum DidaMeetingsAction {OPEN, ADD, TOPIC, CLOSE, DUMP};

public class DidaMeetingsCommand {
    private DidaMeetingsAction  action;
    private int meeting_id;
    private int participant_id;
    private int topic_id;
    
    public DidaMeetingsCommand(DidaMeetingsAction command_type) {
	this.action         = command_type;
 	this.meeting_id     = 0;
	this.participant_id = 0;
	this.topic_id       = -1;
    }
 
    public DidaMeetingsCommand(DidaMeetingsAction command_type, int mid) {
	this.action         = command_type;
	this.meeting_id     = mid;
	this.participant_id = 0;
	this.topic_id       = -1;
    }
  
    public DidaMeetingsCommand(DidaMeetingsAction command_type, int mid, int pid) {
	this.action         = command_type;
        this.meeting_id     = mid;
	this.participant_id = pid;
	this.topic_id       = -1;
    }
 
    public DidaMeetingsCommand(DidaMeetingsAction command_type, int mid, int pid, int tid) {
	this.action         = command_type;
        this.meeting_id     = mid;
	this.participant_id = pid;
	this.topic_id       = tid;
    }
    
    // Getter methods for all fields
    public DidaMeetingsAction getAction() {
        return this.action;
    }
    
    public int getMeetingId() {
        return this.meeting_id;
    }

    public int getParticipantId() {
        return this.participant_id;
    }

    public int getTopicId() {
        return this.topic_id;
    }
}
