package messagefutures.node;

import messagefutures.common.*;
import messagefutures.message.*;
import messagefutures.event.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;


public class AutoClient {

	private String EXCHANGE_NAME = null;
	ConnectionFactory factory = new ConnectionFactory();
	private Connection connection ;
	private  Channel channel;
	//private QueueingConsumer consumer ;
	private List<String> exchanges;
	protected final static Logger LOGGER = Logger.getLogger(Datacentre.class.getName());
	private static FileHandler logFile;
	private static String replayFile ;
	private Timestamp starttime ;
	private static boolean istimeset ;
	
	public AutoClient(String exchName, String replayFile) throws IOException
	{
		this.EXCHANGE_NAME = exchName;
		factory = new ConnectionFactory();
		factory.setHost(Common.RMQServer);
		connection = factory.newConnection();
		channel = connection.createChannel();
		exchanges = new ArrayList<String>();
		this.replayFile = replayFile;
		logFile = new FileHandler(Common.FilePath+"/"+"Auto_"+this.EXCHANGE_NAME+".log", true);
		logFile.setFormatter(new SimpleFormatter());
		LOGGER.setLevel(Level.INFO); //Sets the default level if not provided.		
		LOGGER.addHandler(logFile);
		LOGGER.setUseParentHandlers(false);
		istimeset = false;
	}



	public void populateNodes(List<String> list)
	{
		for(String node: list)
			exchanges.add(node);
	}

	public void AddLogEntry(String message, Level level){		
		LOGGER.logp(level, this.getClass().toString(), "", message);		
	}

	public void sendTestMessage(String dest) throws IOException
	{		
		channel.basicPublish("",dest, null, "test msg".getBytes());
	}

	public void sendLogPropMessage(String dest) throws IOException
	{
		ClientMessage msg = new ClientMessage(EXCHANGE_NAME, MessageType.PROPLOG);
		//this.AddLogEntry("Sent Log Propagation Request " + msg ,Level.INFO);
		//System.out.println("Sent Log Propagation Request " + msg);
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		channel.basicPublish("",dest, null, msgwrap.getSerializedMessage().getBytes());
	}

	public void sendBeginMessage(String dest) throws IOException
	{
		ClientMessage msg = new ClientMessage(EXCHANGE_NAME, MessageType.BEGIN);
		this.AddLogEntry("Sent Begin Request " + msg ,Level.INFO);
		System.out.println("Sent Begin Request " + msg);
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		channel.basicPublish("",dest, null, msgwrap.getSerializedMessage().getBytes());
	}

	public void sendReadMessage(Integer transNo,String readObject, String dest) throws IOException
	{
		ClientMessage msg = new ClientMessage(EXCHANGE_NAME, MessageType.READ,transNo,readObject);
		this.AddLogEntry("Sent Read Request " + msg ,Level.INFO);
		System.out.println("Sent Read Request " + msg);
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		channel.basicPublish("",dest, null, msgwrap.getSerializedMessage().getBytes());		
	}


	public void sendWriteMessage(String writeObject,String value, String dest,Integer transNo) throws IOException
	{
		ClientMessage msg = new ClientMessage(EXCHANGE_NAME, MessageType.WRITE,transNo,writeObject,value);
		this.AddLogEntry("Sent Write Request " + msg ,Level.INFO);
		System.out.println("Sent Write Request " + msg);
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		channel.basicPublish("",dest, null, msgwrap.getSerializedMessage().getBytes());		
	}


	public void sendCommitMessage(Integer transNumber, String dest) throws IOException
	{
		ClientMessage msg = new ClientMessage(EXCHANGE_NAME, MessageType.COMMIT,transNumber);
		this.AddLogEntry("Sent Commit Request " + msg ,Level.INFO);
		System.out.println("Sent Commit Request " + msg);
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		channel.basicPublish("",dest, null, msgwrap.getSerializedMessage().getBytes());		
	}

	public void sendAbortMessage(Integer transNumber, String dest) throws IOException
	{
		ClientMessage msg = new ClientMessage(EXCHANGE_NAME, MessageType.ABORT,transNumber);
		this.AddLogEntry("Sent Abort Request " + msg ,Level.INFO);
		System.out.println("Sent Abort Request " + msg);
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		channel.basicPublish("",dest, null, msgwrap.getSerializedMessage().getBytes());
	}


	public void sendScanMessage(String dest) throws IOException
	{
		ClientMessage msg = new ClientMessage(EXCHANGE_NAME, MessageType.SCAN);
		this.AddLogEntry("Sent Scan Request " + msg ,Level.INFO);
		System.out.println("Sent Scan Request " + msg);
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		channel.basicPublish("",dest, null, msgwrap.getSerializedMessage().getBytes());
	}

	public void sendListActiveMessage(String dest) throws IOException
	{
		ClientMessage msg = new ClientMessage(EXCHANGE_NAME, MessageType.LISTACTIVE);
		this.AddLogEntry("Sent ListActive Request " + msg ,Level.INFO);
		System.out.println("Sent ListActive Request " + msg);
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		channel.basicPublish("",dest, null, msgwrap.getSerializedMessage().getBytes());
	}


	public void sendListPendingMessage(String dest) throws IOException
	{
		ClientMessage msg = new ClientMessage(EXCHANGE_NAME, MessageType.LISTPENDING);
		this.AddLogEntry("Sent ListPending Request " + msg ,Level.INFO);
		System.out.println("Sent ListPending Request " + msg);
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		channel.basicPublish("",dest, null, msgwrap.getSerializedMessage().getBytes());
	}


	public void sendMessages() throws IOException, InterruptedException
	{
		try {

			Scanner scanner = new Scanner(new File(replayFile));
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				if(line != null || !line.equals("") || (line.charAt(0) =='A')||(line.charAt(0) =='B')|| (line.charAt(0) =='C')) {
					StringTokenizer str = new StringTokenizer(line," |\t");
					String dest = str.nextToken();
					Integer opt = Integer.parseInt(str.nextToken());                
					System.out.println(line);
					if(!istimeset){
						starttime = new Timestamp(new Date().getTime());
						istimeset = true;
					}
					switch(opt)
					{
					case 1:
						this.sendBeginMessage(dest+"Queue");
						break;
					case 2:					
						Integer transNo = Integer.parseInt(str.nextToken());
						System.out.println("Txn No "+transNo);
						this.AddLogEntry("Txn No : "+transNo.toString(), Level.INFO);					
						String message = str.nextToken();
						System.out.println("Object to read "+message);
						this.AddLogEntry("Object to read : "+message, Level.INFO);
						this.sendReadMessage(transNo,message, dest+"Queue");
						break;
					case 3:	
						transNo = Integer.parseInt(str.nextToken());
						System.out.println("Txn No "+transNo);
						this.AddLogEntry("Txn No : "+transNo.toString(), Level.INFO);					
						message = str.nextToken();
						System.out.println("Object to write "+message);
						this.AddLogEntry("Object to write : "+message, Level.INFO);
						String value = str.nextToken();
						System.out.println("Value to write "+value);
						this.AddLogEntry("Value to write : "+value, Level.INFO);
						this.sendWriteMessage(message, value, dest+"Queue",transNo);
						break;
					case 4:
						transNo = Integer.parseInt(str.nextToken());
						System.out.println("Txn No to Commit "+transNo);
						this.AddLogEntry("Txn No to Commit : "+transNo.toString(), Level.INFO);
						this.sendCommitMessage(transNo, dest+"Queue");
						break;
					case 5:
						transNo = Integer.parseInt(str.nextToken());
						System.out.println("Txn No to Abort "+transNo);
						this.AddLogEntry("Txn No to Abort : "+transNo.toString(), Level.INFO);
						this.sendAbortMessage(transNo, dest+"Queue");
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
						this.sendLogPropMessage(dest+"Queue");
						break;
					default:
						break;
					}
				}
				Timestamp curtime =  new Timestamp(new Date().getTime());
				System.out.println("Time taken "+(curtime.getTime() - starttime.getTime()));
				this.AddLogEntry("Time taken "+(curtime.getTime() - starttime.getTime()), Level.INFO);
				Thread.sleep(10);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
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

			System.out.println("1 - Begin Trans, 2 - Read, 3 - Write, 4 - Commit, 5 - Abort, 6 - Scan, 7 - List Active, 8 - List Pending, 9 - Trigger Propagation of Log, 10 - main menu");

			option = Integer.parseInt(br.readLine());
			if(option != 10)
			{
				switch(option){
				case 1:
					this.sendBeginMessage(dest+"Queue");
					break;
				case 2:
					System.out.println("Enter the trans number ");
					this.AddLogEntry("Enter the trans number", Level.INFO);
					Integer transNo = Integer.parseInt(br.readLine());
					this.AddLogEntry(transNo.toString(), Level.INFO);
					System.out.println("Enter the Object to read ");
					this.AddLogEntry("Enter the Object to read", Level.INFO);
					String message = br.readLine();
					this.AddLogEntry(message, Level.INFO);
					this.sendReadMessage(transNo,message, dest+"Queue");
					break;
				case 3:
					System.out.println("Enter the trans number ");
					this.AddLogEntry("Enter the trans number", Level.INFO);
					transNo = Integer.parseInt(br.readLine());
					this.AddLogEntry(transNo.toString(), Level.INFO);
					System.out.println("Enter the Object to write ");
					this.AddLogEntry("Enter the Object to write", Level.INFO);
					message = br.readLine();
					this.AddLogEntry(message, Level.INFO);
					System.out.println("Enter the value to write");
					this.AddLogEntry("Enter the value to write", Level.INFO);
					String val = br.readLine();
					this.AddLogEntry(val, Level.INFO);
					this.sendWriteMessage(message, val, dest+"Queue",transNo);
					break;
				case 4:
					System.out.println("Enter the Trans Number to Commit ");
					this.AddLogEntry("", Level.INFO);
					Integer toCommit = Integer.parseInt(br.readLine());
					this.AddLogEntry(toCommit.toString(), Level.INFO);
					this.sendCommitMessage(toCommit, dest+"Queue");
					break;
				case 5:
					System.out.println("Enter the Trans Number to Abort ");
					this.AddLogEntry("", Level.INFO);
					Integer toAbort = Integer.parseInt(br.readLine());
					this.AddLogEntry(toAbort.toString(), Level.INFO);
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
					this.sendLogPropMessage(dest+"Queue");
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

		if(argv.length != 3){
			System.out.println("Usage [NodeName] [Nodes in the system separated by comma] [Replay file]");
			throw new IllegalArgumentException();
		}
		AutoClient obj = new AutoClient(argv[0],argv[2]);
		String[] allnodes = argv[1].split(",");
		obj.populateNodes(Arrays.asList(allnodes));	
		obj.sendMessages();
	}

}
