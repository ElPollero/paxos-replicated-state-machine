package didameetings.core;

import java.util.*;

public class Meeting {
    private int id;
    private int max;
    private int n_participants;
    private boolean closed;
    private Hashtable<Integer, Participant> without_topic;
    private Hashtable<Integer, Participant> with_topic;

    public Meeting(int id, int max) {
	this.id             = id;
	this.max            = max;
	this.n_participants = 0;
	this.closed         = false;
	this.without_topic  = new Hashtable<Integer, Participant>();
	this.with_topic     = new Hashtable<Integer, Participant>();;
    }

    public int max () {
	return this.max;
    }

    public int getId () {
	return this.id;
    }

    public boolean add (Integer id) {
	Participant p;
	
	if (this.closed || (this.n_participants >= this.max))
	    return false;

	if (this.without_topic.get(id)!=null)
	    return false;
	
	if (this.with_topic.get(id)!=null)
	    return false;

	p = new Participant (id);
	this.without_topic.put (id, p);
	this.n_participants++;
	return true;
    }

    public boolean setTopic (Integer pid, int topic) {
	Participant p = this.without_topic.get(pid);

	if (p==null)
	    p = this.with_topic.get(pid);
	if (p==null)
	    return false;
	else {
	    p.setTopic(topic);
	    this.with_topic.put(pid, p);
	    this.without_topic.remove(pid);
	    return true;
	}
    }
	
    public int getTopic (Integer pid) {
	Participant p = this.with_topic.get(pid);

	if (p==null)
	    return -1;
	else
	    return p.getTopic();
    }
    
    public boolean participantExists (Integer pid) {
	return this.without_topic.containsKey(pid) || this.with_topic.containsKey(pid);
    }
    
    public boolean close () {
	if (!this.closed) {
	    this.closed = true;
	    return true;
	}
	else
	    return false;
    }

    public Participant getParticipant (int id) {
	Participant p = this.without_topic.get(id);

	if (p==null)
	    p = this.with_topic.get(id);

	return p;
    }

    public Enumeration<Integer> participantsWithTopic () {
	return this.with_topic.keys();
    }
	 

    public Enumeration<Integer> participantsWithoutTopic () {
	return this.without_topic.keys();
    }

    public int numberOfParticipantsWithoutTopic () {
	return this.without_topic.size();
    }

    public int size () {
	return this.n_participants;
    }
}
