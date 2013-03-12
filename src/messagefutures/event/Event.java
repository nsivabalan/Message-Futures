package messagefutures.event;

import java.util.HashMap;

public class Event {	
	
	protected EventType type;
	protected  HashMap<String,HashMap<String,String>> writeSet;
	
	public Event(EventType type)
	{		
		this.type=type;	
		writeSet = new HashMap<String,HashMap<String,String>>();
	}
	
	public Event(EventType type,HashMap<String,HashMap<String,String>> writeset)
	{		
		this.type=type;	
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

	public HashMap<String, HashMap<String, String>> getWriteSet() {
		return writeSet;
	}

	public void setWriteSet(HashMap<String, HashMap<String, String>> writeSet) {
		this.writeSet = writeSet;
	}
	
	
	
}

