package didameetings.core;

import java.util.*;

public class MeetingManager {
    private Hashtable<Integer, Meeting> open_meetings;
    private Hashtable<Integer, Meeting> closed_meetings;

    public MeetingManager() {
	this.open_meetings = new Hashtable<Integer, Meeting>();
	this.closed_meetings = new Hashtable<Integer, Meeting>();
    }

    public boolean open (Integer mid, int max) {
	Meeting new_meeting = this.open_meetings.get(mid);

	if (new_meeting != null) 
	    return false;

	new_meeting = this.closed_meetings.get(mid);
	if (new_meeting != null)
	    return false;

	new_meeting = new Meeting (mid, max);
	this.open_meetings.put (mid, new_meeting);
	return true;
    }

    public boolean add (Integer mid, Integer pid) {
	Meeting meeting = this.open_meetings.get(mid);

	if (meeting == null) 
	    return false;
	
	return meeting.add (pid);
    }

    public boolean setTopic (Integer mid, Integer pid, int topic_id) {
	Meeting meeting = this.open_meetings.get(mid);

	if (meeting == null) 
	    meeting = this.closed_meetings.get(mid);
	
	if (meeting == null) 
	    return false;
	
	return meeting.setTopic (pid, topic_id);
    }
    
    public boolean meetingExists (Integer mid) {
	return this.open_meetings.containsKey(mid) || this.closed_meetings.containsKey(mid);
    }

    public boolean participantExists (Integer mid, Integer pid) {
	Meeting meeting = this.open_meetings.get(mid);
	if (meeting == null) 
	    meeting = this.closed_meetings.get(mid);
	
	if (meeting == null) 
	    return false;
	
	return meeting.participantExists(pid);
    }

    public boolean addAndClose (Integer mid, Integer pid) {
	Meeting meeting = this.open_meetings.get(mid);

	if (meeting != null) {
	    meeting.add (pid);
	    if (meeting.size() == meeting.max()) {
		meeting.close();
		this.closed_meetings.put (mid, meeting);
		this.open_meetings.remove (mid);
	    }
	    return true;
	}
	else
	    return false;
    }

    public boolean close (Integer mid) {
	Meeting meeting = this.closed_meetings.get(mid);

	if (meeting != null)
	    return true;

	meeting = this.open_meetings.get(mid);

	if (meeting == null) 
	    return false;
	
	meeting.close();

	this.closed_meetings.put (mid, meeting);
	this.open_meetings.remove (mid);
	return true;
    }


    
    public Enumeration<Integer> participantsWithTopic (int mid) {
	Meeting meeting = this.open_meetings.get(mid);
	if (meeting == null) 
	    meeting = this.closed_meetings.get(mid);
	if (meeting == null)
	    return null;
	else
	    return meeting.participantsWithTopic();
    }

    public Enumeration<Integer> participantsWithoutTopic (int mid) {
	Meeting meeting = this.open_meetings.get(mid);
	if (meeting == null) 
	    meeting = this.closed_meetings.get(mid);
	if (meeting == null)
	    return null;
	else
	    return meeting.participantsWithoutTopic();
    }
	 

    public boolean dump () {
	Enumeration<Integer> ids;
	Integer mid;
	Integer pid;
	int     topic;
	Meeting m;
	Enumeration<Integer> plist;
	
	System.out.println("\n ----------- Open meetings ----------- \n");

	ids = this.open_meetings.keys();
	while (ids.hasMoreElements()) {
	    mid = new Integer (ids.nextElement());
	    m = this.open_meetings.get(mid);
	    
	    System.out.println("\n\t Meeting " + mid);
	    
	    System.out.println("\n\t\t with topic: ");
	    plist = m.participantsWithTopic();
	    while (plist.hasMoreElements()) {
		pid = new Integer (plist.nextElement());
		topic = m.getTopic (pid);
		System.out.println("\n\t\t\t(" + pid + "," + topic + ") ");
	    }
	    
	    System.out.println("\n\t\t without topic: ");
	    plist = m.participantsWithoutTopic();
	    while (plist.hasMoreElements()) {
		pid = new Integer (plist.nextElement());
		System.out.println("\n\t\t\t(" + pid + ") ");
	    }	    
	}
	
	System.out.println("\n ----------- Closed meetings ----------- \n");

	ids = this.closed_meetings.keys();
	while (ids.hasMoreElements()) {
	    mid = new Integer (ids.nextElement());
	    m = this.closed_meetings.get(mid);
	    
	    System.out.println("\n\t Meeting " + mid);
	    
	    System.out.println("\n\t\t with topic: ");
	    plist = m.participantsWithTopic();
	    while (plist.hasMoreElements()) {
		pid = new Integer (plist.nextElement());
		topic = m.getTopic (pid);
		System.out.println("\n\t\t\t(" + pid + "," + topic + ") ");
	    }
	    
	    System.out.println("\n\t\t without topic: ");
	    plist = m.participantsWithoutTopic();
	    while (plist.hasMoreElements()) {
		pid = new Integer (plist.nextElement());
		System.out.println("\n\t\t\t(" + pid + ") ");
	    }
	}

	
	System.out.println("\n -----------    done     ----------- \n");

	return true;
    } 
}

    
