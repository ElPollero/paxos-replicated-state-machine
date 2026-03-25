package didameetings.core;

import java.util.*;

public class Participant {
    private int id;
    private int topic;

    public Participant(int id) {
	this.id     = id;
	this.topic  = -1;
    }

    public Participant(int id, int topic) {
	this.id     = id;
	this.topic  = topic;
    }
    
    public int getId () {
	return this.id;
    }
    
    public int getTopic () {
	return this.topic;
    }
    
    public void setTopic (int topic) {
	if (topic > this.topic)
	    this.topic = topic;
    }
}
