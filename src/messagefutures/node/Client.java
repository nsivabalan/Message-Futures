package messagefutures.node;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import messagefutures.common.*;
import messagefutures.message.*;
import messagefutures.event.*;
public class Client {

	private String EXCHANGE_NAME = null;
	ConnectionFactory factory = new ConnectionFactory();
	private Connection connection ;
	private  Channel channel;
	private QueueingConsumer consumer ;
	private List<String> exchanges;
	public Client(String exchName) throws IOException
	{
		this.EXCHANGE_NAME = exchName;
		factory = new ConnectionFactory();
		factory.setHost(Common.RMQServer);
		connection = factory.newConnection();
		channel = connection.createChannel();
		exchanges = new ArrayList<String>();
	}



	public void populateNodes(List<String> list)
	{
		for(String node: list)
			exchanges.add(node);
	}
	
	
	public void sendTestMessage(String dest) throws IOException
	{
		
		channel.basicPublish("",dest, null, "test msg".getBytes());
	}
	
	
	public void sendBeginMessage(String dest) throws IOException
	{

		ClientOpMessage msg = new ClientOpMessage(EXCHANGE_NAME, MessageType.BEGIN);
		System.out.println(" [x] Sent '" + msg + "'");
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		channel.basicPublish("",dest, null, msgwrap.getSerializedMessage().getBytes());
	}

	public void sendReadMessage(Integer transNo,String readObject,String qualifier, String dest) throws IOException
	{

		ClientOpMessage msg = new ClientOpMessage(EXCHANGE_NAME, MessageType.READ,transNo,readObject,qualifier);
		System.out.println(" [x] Sent '" + msg + "'");
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		channel.basicPublish("",dest, null, msgwrap.getSerializedMessage().getBytes());
		
	}
	
	
	public void sendWriteMessage(String writeObject,String qual,String value, String dest,Integer transNo) throws IOException
	{

		ClientOpMessage msg = new ClientOpMessage(EXCHANGE_NAME, MessageType.WRITE,transNo,writeObject, qual,value);
		System.out.println(" [x] Sent '" + msg + "'");
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		channel.basicPublish("",dest, null, msgwrap.getSerializedMessage().getBytes());
		
	}

	
	public void sendCommitMessage(Integer transNumber, String dest) throws IOException
	{

		ClientOpMessage msg = new ClientOpMessage(EXCHANGE_NAME, MessageType.COMMIT,transNumber);
		System.out.println(" [x] Sent '" + msg + "'");
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		channel.basicPublish("",dest, null, msgwrap.getSerializedMessage().getBytes());
		
	}
	
	public void sendAbortMessage(Integer transNumber, String dest) throws IOException
	{

		ClientOpMessage msg = new ClientOpMessage(EXCHANGE_NAME, MessageType.ABORT,transNumber);
		System.out.println(" [x] Sent '" + msg + "'");
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		channel.basicPublish("",dest, null, msgwrap.getSerializedMessage().getBytes());
	}
	
	
	public void sendScanMessage(String dest) throws IOException
	{

		ClientOpMessage msg = new ClientOpMessage(EXCHANGE_NAME, MessageType.SCAN);
		System.out.println(" [x] Sent '" + msg + "'");
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		channel.basicPublish("",dest, null, msgwrap.getSerializedMessage().getBytes());
	}
	
	public void sendListActiveMessage(String dest) throws IOException
	{

		ClientOpMessage msg = new ClientOpMessage(EXCHANGE_NAME, MessageType.LISTACTIVE);
		System.out.println(" [x] Sent '" + msg + "'");
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		channel.basicPublish("",dest, null, msgwrap.getSerializedMessage().getBytes());
	}
	
	
	public void sendListPendingMessage(String dest) throws IOException
	{

		ClientOpMessage msg = new ClientOpMessage(EXCHANGE_NAME, MessageType.LISTPENDING);
		System.out.println(" [x] Sent '" + msg + "'");
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		channel.basicPublish("",dest, null, msgwrap.getSerializedMessage().getBytes());
	}
	
	
	
	public void sendMsgs() throws NumberFormatException, IOException
	{
		Integer option = null;
		String dest = null;
		do{
			System.out.print("Choose Destination :: ");
			for(String node : exchanges)
				System.out.print(" "+node);
			System.out.println();
			
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			dest = br.readLine();

			System.out.println("1 - Begin Trans, 2 - Read, 3 - Write, 4 - Commit, 5 - Abort, 6 - Scan, 7 - List Active, 8 - List Pending, 9 - Test Message, 10 - main menu");

			option = Integer.parseInt(br.readLine());
			if(option != 10)
			{
				switch(option){
				case 1:
					this.sendBeginMessage(dest+"Queue");
					break;
				case 2:
					System.out.println("Enter the trans number ");
					Integer transNo = Integer.parseInt(br.readLine());
					System.out.println("Enter the Object to read ");
					String message = br.readLine();
					System.out.println("Enter the qualifier");
					String qual = br.readLine();
					this.sendReadMessage(transNo,message,qual, dest+"Queue");
					break;
				case 3:
					System.out.println("Enter the trans number ");
					transNo = Integer.parseInt(br.readLine());
					System.out.println("Enter the Object to write ");
					message = br.readLine();
					System.out.println("Enter the qualifier");
					qual = br.readLine();
					System.out.println("Enter the value to over write");
					String val = br.readLine();
					this.sendWriteMessage(message,qual, val, dest+"Queue",transNo);
					break;
				case 4:
					System.out.println("Enter the Trans Number to Commit ");
					Integer toCommit = Integer.parseInt(br.readLine());
					this.sendCommitMessage(toCommit, dest+"Queue");
					break;
				case 5:
					System.out.println("Enter the Trans Number to Abort ");
					Integer toAbort = Integer.parseInt(br.readLine());
					this.sendAbortMessage(toAbort, dest+"Queue");
					break;
				case 6:
					this.sendScanMessage(dest+"Queue");
					break;
				case 7:
					this.sendListActiveMessage(dest+"Queue");
					break;
				case 8:
					this.sendListPendingMessage(dest+"Queue");
					break;
				case 9:
					this.sendTestMessage(dest+"Queue");
					break;
				default:
					break;	
				}
			}
			else
				break;
		}while(true);

	}


	public void finalize() throws IOException
	{

		channel.close();
		connection.close();
	}


	public static void main(String[] argv) throws Exception {

		if(argv.length != 2){
			System.out.println("Usage [NodeName] [Nodes in the system separated by comma]");
			throw new IllegalArgumentException();
		}

		Client obj = new Client(argv[0]);
		String[] allnodes = argv[1].split(",");
		obj.populateNodes(Arrays.asList(allnodes));
	
		obj.sendMsgs();

	}

}