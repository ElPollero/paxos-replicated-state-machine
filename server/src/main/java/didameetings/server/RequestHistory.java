
package didameetings.server;

import java.util.Hashtable;
import java.util.LinkedHashMap;

public class RequestHistory {
    private LinkedHashMap<Integer, RequestRecord> pending;
    private Hashtable<Integer, RequestRecord> processed;
    private Hashtable<Integer, RequestRecord> proposed;
     
    public RequestHistory() {
	this.pending = new LinkedHashMap<Integer, RequestRecord>();
        this.processed = new Hashtable<Integer, RequestRecord>();
        this.proposed = new Hashtable<Integer, RequestRecord>();
    }

     public synchronized RequestRecord getIfPending(int requestid) {
	Integer id = Integer.valueOf(requestid);
        return this.pending.get(id);
    }
   
    public synchronized RequestRecord getFirstPending() {
        // LinkedHashMap preserves insertion order - get first entry
        for (RequestRecord record : this.pending.values()) {
            return record;  // Return first (oldest) pending request
        }
        return null;  // No pending requests
    }
   
    public synchronized RequestRecord getIfProcessed(int requestid) {
	Integer id = Integer.valueOf(requestid);
        return this.processed.get(id);
    }
   
    public synchronized RequestRecord getIfExists(int requestid) {
        RequestRecord record;
	Integer id = Integer.valueOf(requestid);

	record = this.pending.get(id);
	if (record == null) {
	    record = this.proposed.get(id);
	    if (record == null) {
	        record = this.processed.get(id);
	    }
	}
	return record;
    }
   
    public synchronized void addToPending(int requestid, RequestRecord record) {
	Integer id = Integer.valueOf(requestid);
	
	this.pending.put (id, record);
    }

    /**
     * Checks if a request is currently in the proposed state
     * @param requestid The ID of the request to check
     * @return true if the request is in proposed state, false otherwise
     */
    public synchronized boolean isInProposed(int requestid) {
        Integer id = Integer.valueOf(requestid);
        return this.proposed.containsKey(id);
    }

    public synchronized RequestRecord moveToProcessed(int requestid) {
	Integer id = Integer.valueOf(requestid);
        RequestRecord record = this.pending.remove(id);
        if (record == null) {
            // Check if it was in proposed state
            record = this.proposed.remove(id);
        }
	this.processed.put(id, record);
	return record;
    }
    
    /**
     * Moves a request from pending to proposed state
     * Called after a request has been successfully proposed in a Paxos instance
     * @param requestid The ID of the request to mark as proposed
     * @return The request record that was moved, or null if not found
     */
    public synchronized RequestRecord moveToProposed(int requestid) {
        Integer id = Integer.valueOf(requestid);
        RequestRecord record = this.pending.remove(id);
        if (record != null) {
            this.proposed.put(id, record);
            return record;
        } else {
            // If already in proposed state, return it
            return this.proposed.get(id);
        }
    }
    /** Busca um RequestRecord pelo command_id (usado na entrega em ordem). */
    public synchronized RequestRecord getByCommandId(int commandId) {
        return getIfExists(commandId);
    }
    
    /**
     * Checks if there's a pending OR proposed OPEN request for the specified meeting ID
     * @param meetingId The meeting ID to check for
     * @return true if there's a pending or proposed OPEN request for this meeting, false otherwise
     */
    public synchronized boolean hasPendingOpenRequest(int meetingId) {
        // Check pending OPEN requests (not yet started Paxos)
        for (RequestRecord record : this.pending.values()) {
            DidaMeetingsCommand command = record.getRequest();
            if (command.getAction() == DidaMeetingsAction.OPEN && 
                command.getMeetingId() == meetingId) {
                return true;
            }
        }
        
        // Check proposed OPEN requests (Paxos decided but not executed yet)
        for (RequestRecord record : this.proposed.values()) {
            DidaMeetingsCommand command = record.getRequest();
            if (command.getAction() == DidaMeetingsAction.OPEN && 
                command.getMeetingId() == meetingId) {
                return true;
            }
        }
        
        return false;
    }
        
}
