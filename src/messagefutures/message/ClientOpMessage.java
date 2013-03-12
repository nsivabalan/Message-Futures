package messagefutures.message;


public class ClientOpMessage extends MessageBase{

	public Integer TransactionNo = null;
	public String readObject = null;
	public String writeObject = null;
	public String qualifier = null;
	public String value = null;
	public MessageType type = null;
	public String source = null;
	//begin trans
	public ClientOpMessage(String source, MessageType type)
	{
		this.source = source;
		this.type = type;
	}
	
	//read
	public ClientOpMessage(String source,MessageType type,Integer transNo, String readObject,String qualifier)
	{
		this.source = source;
		this.type = type;
		this.TransactionNo = transNo;
		this.readObject = readObject;
		this.qualifier = qualifier;
	}
	
	//write
	public ClientOpMessage(String source,MessageType type,Integer transNo, String writeObject, String qual, String value)
	{
		this.source = source;
		this.type = type;
		this.TransactionNo = transNo;
		this.writeObject = writeObject;
		this.qualifier = qual;
		this.value = value;
	}
	
	//commit or abort
	public ClientOpMessage(String source,MessageType type, Integer transNo)
	{
		this.source = source;
		this.type = type;
		this.TransactionNo = transNo;
	}
	

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		StringBuffer buffer = new StringBuffer();
		
		
		buffer.append("Source : "+source+" \n");
		buffer.append("Message Type : "+type+" \n");
		if(TransactionNo != null)
			buffer.append("Trans Number : "+TransactionNo+" \n");
		if(readObject != null)
			buffer.append("Read Object : "+readObject+" \n");
		if(writeObject != null)
			buffer.append("Write Object : "+writeObject+" \n");
		if(qualifier != null)
			buffer.append("Qualifier : "+qualifier+" \n");
		if(value != null)
			buffer.append("Value : "+value+" \n");
		
		return buffer.toString();
	}
	
	
	
}
