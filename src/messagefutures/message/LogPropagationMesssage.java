package messagefutures.message;

import messagefutures.event.*;
import java.util.HashMap;

public class LogPropagationMesssage extends MessageBase{

	public String source;
	public MessageType type = MessageType.LOGPROPAGATE;
	public HashMap<Integer,HashMap<Integer, HashMap<Integer, Event>>> logDetails;
	public int[][] log ;
	public LogPropagationMesssage(String source){
		this.source = source;
	}

	public LogPropagationMesssage(String source, HashMap<Integer,HashMap<Integer, HashMap<Integer, Event>>> logDetails,int[][] log)
	{
		this.source = source;
		this.logDetails = logDetails;
		this.log = log;
	}
	
	

	
	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("Source : "+source+" \n");
		/*if(logDetails != null)
			buffer.append(logDetails +"\n");*/
		if(log != null){
			for(int i =0; i< log.length ;i++){
				for(int j = 0;j< log[i].length;j++)
				buffer.append(" "+log[i][j]);
			buffer.append("\n");
			}
		}
				
		return new String(buffer);
	}
	
}
