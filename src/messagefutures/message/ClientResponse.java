package messagefutures.message;

public class ClientResponse extends MessageBase{
	

		public Integer TransactionNo = null;
		public MessageType type = null;
		public String source = null;
		public String msg = null;
		public String val = null;
		
		public ClientResponse(String source, MessageType type, Integer transNo)
		{
			this.source = source;
			this.type = type;
			this.TransactionNo = transNo;
		}
		
		public ClientResponse(String source, MessageType type, Integer transNo,String key)
		{
			this.source = source;
			this.type = type;
			this.TransactionNo = transNo;
			this.msg = key;
		}
		
		public ClientResponse(String source, MessageType type, Integer transNo,String key,String value)
		{
			this.source = source;
			this.type = type;
			this.TransactionNo = transNo;
			this.msg = key;
			this.val = value;
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
			if(TransactionNo != null)
			buffer.append("Trans Number : "+TransactionNo+" \n");			
			if(msg != null)
				buffer.append(msg+" ");
			if(val != null)
				buffer.append(val);
			buffer.append("\n");
			return buffer.toString();
		}
		
	

		
	}
