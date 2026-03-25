package didameetings.server;


public class RequestRecord {
    private int requestid;
    private DidaMeetingsCommand request;
    private boolean response_available;
    private boolean response_value;
    private int instanceNumber;  // Pre-assigned instance number for consistent ordering
    private int logicalTimestamp = -1;  // Logical timestamp when command was executed
    
    public RequestRecord(int id) {
	this.requestid = id;
        this.request = null;
	this.response_available = false;
	this.response_value = false;
        this.instanceNumber = -1;  // Will be set later
    }
   
    public RequestRecord(int id, DidaMeetingsCommand rq) {
	this.requestid = id;
        this.request = rq;
	this.response_available = false;
 	this.response_value = false;
        this.instanceNumber = -1;  // Will be set later
    }

    public RequestRecord(int id, DidaMeetingsCommand rq, int instance) {
	this.requestid = id;
        this.request = rq;
	this.response_available = false;
 	this.response_value = false;
        this.instanceNumber = instance;  // Pre-assigned instance
    }
    
    // Getter and Setter methods for all fields
    public DidaMeetingsCommand getRequest() {
        return this.request;
    }
    
    public int getId() {
        return this.requestid;
    }

    public int getInstanceNumber() {
        return this.instanceNumber;
    }

    public void setInstanceNumber(int instance) {
        this.instanceNumber = instance;
    }

    public void setRequest(DidaMeetingsCommand rq) {
        this.request = rq;
    }

    public int getLogicalTimestamp() {
        return this.logicalTimestamp;
    }

    public void setLogicalTimestamp(int timestamp) {
        this.logicalTimestamp = timestamp;
    }

    public boolean getResponseValue() {
        return this.response_value;
    }

    public synchronized void setResponse(boolean resp) {
        this.response_value = resp;
	this.response_available = true;
	this.notifyAll();
    }
   
    public synchronized boolean waitForResponse() {
        // Other commands use normal timeout
        final long TIMEOUT_MS = 12000; // 12 seconds - account for leader processing delay under load
        long startTime = System.currentTimeMillis();
        
        while (this.response_available == false) {
            long elapsed = System.currentTimeMillis() - startTime;
            if (elapsed >= TIMEOUT_MS) {
                System.err.println("Request " + this.requestid + " timed out after " + elapsed + "ms");
                // Return false to indicate timeout/failure
                return false;
            }
            
            try {
                // Wait with timeout to periodically check for timeout condition
                long remainingTime = TIMEOUT_MS - elapsed;
                if (remainingTime > 0) {
                    wait(Math.min(remainingTime, 1000)); // Check every second or remaining time
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return this.response_value;
    }
    
}
