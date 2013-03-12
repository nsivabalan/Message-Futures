package messagefutures.message;


public class ClientResponse extends MessageBase{
	

		public Integer TransactionNo = null;
		public MessageType type = null;
		public String source = null;
		public String msg = null;
		
		public ClientResponse(String source, MessageType type, Integer transNo)
		{
			this.source = source;
			this.type = type;
			this.TransactionNo = transNo;
		}
		
		public ClientResponse(String source, MessageType type, Integer transNo,String msg)
		{
			this.source = source;
			this.type = type;
			this.TransactionNo = transNo;
			this.msg = msg;
		}

		public ClientResponse(String source,MessageType type, String scanMsg)
		{
			this.source = source;
			this.type = type;
			this.msg = scanMsg;
		}
		
		@Override
		public String toString() {
			StringBuffer buffer = new StringBuffer();
			buffer.append("Source : "+source+" \n");
			buffer.append("Type : "+type+" \n");
			buffer.append("Trans Number : "+TransactionNo+" \n");			
			if(msg != null)
				buffer.append(msg);
			return buffer.toString();
		}
		
	

		
	}