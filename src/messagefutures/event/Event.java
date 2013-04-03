package messagefutures.event;

import java.util.HashMap;


public class Event {	
	
	protected EventType type;
	protected  HashMap<String,String> writeSet;
	protected String transScource;
	
	public String getTransScource() {
		return transScource;
	}

	public void setTransScource(String transScource) {
		this.transScource = transScource;
	}

	public Event(EventType type, String source)
	{		
		this.type=type;	
		transScource = source;
		writeSet = new HashMap<String,String>();
	}
	
	public Event(EventType type,String source,HashMap<String,String> writeset)
	{		
		this.type=type;	
		transScource = source;
		this.writeSet = writeset;
	}
	
	public EventType getEventType()
	{
		return type;
	}

	public EventType getType() {
		return type;
	}

	public void setType(EventType type) {
		this.type = type;
	}

	public HashMap<String, String> getWriteSet() {
		return writeSet;
	}

	public void setWriteSet(HashMap<String, String> writeSet) {
		this.writeSet = writeSet;
	}
	
	
	
}
