package messagefutures.node;

import messagefutures.common.*;
import messagefutures.message.*;
import messagefutures.event.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
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
import java.util.HashMap;

public class AutoClientReceiver implements Runnable {

	private String EXCHANGE_NAME = null;
	ConnectionFactory factory = new ConnectionFactory();
	private Connection connection ;
	private  Channel channel;
	private QueueingConsumer consumer ;
	private List<String> exchanges;
	protected final static Logger LOGGER = Logger.getLogger(Datacentre.class.getName());
	private static FileHandler logFile;
	private static String replayFile;
	private static ConcurrentHashMap<String,HashMap<String,String>> readvalues ;
	private static Integer missCount = 0;
	private Timestamp starttime ;
	private static boolean istimeset ;	

	public AutoClientReceiver(String exchName,String replayFile) throws IOException
	{
		this.EXCHANGE_NAME = exchName;
		factory = new ConnectionFactory();
		factory.setHost(Common.RMQServer);
		connection = factory.newConnection();
		channel = connection.createChannel();
		exchanges = new ArrayList<String>();
		this.replayFile = replayFile;
		readvalues =  new ConcurrentHashMap<String,HashMap<String,String>>();
		logFile = new FileHandler(Common.FilePath+"/"+"AutoReceiver_"+this.EXCHANGE_NAME+"_Receiver"+".log", true);
		logFile.setFormatter(new SimpleFormatter());
		LOGGER.setLevel(Level.INFO); //Sets the default level if not provided.		
		LOGGER.addHandler(logFile);
		LOGGER.setUseParentHandlers(false);
		istimeset = false;
	}



	public void declareQueue() throws IOException
	{
		channel.queueDeclare(EXCHANGE_NAME+"Queue", false, false, false, null);
		consumer = new QueueingConsumer(channel);
		channel.basicConsume(EXCHANGE_NAME+"Queue", true, consumer);
	}

	public void populateNodes(List<String> list)
	{
		for(String node: list){
			exchanges.add(node);
			readvalues.put(node, new HashMap<String,String>());
		}
	}

	public void AddLogEntry(String message, Level level){		
		LOGGER.logp(level, this.getClass().toString(), "", message);		
	}

	public void run()
	{
		System.out.println("Waiting for messages. To exit press CTRL+C");
		MessageWrapper wrapper = null;
		while (true) {	 
			QueueingConsumer.Delivery delivery;
			try {
				delivery = consumer.nextDelivery();
				if(delivery != null)
				{
					String msg = new String(delivery.getBody());
					wrapper = MessageWrapper.getDeSerializedMessage(msg); 

					if (wrapper != null)
					{
						if (wrapper.getmessageclass() == ClientResponse.class )
						{
							if(!istimeset){
								starttime = new Timestamp(new Date().getTime());
								istimeset = true;
							}

							ClientResponse response = (ClientResponse) wrapper.getDeSerializedInnerMessage();
							this.AddLogEntry("Response Received ::\n"+response,Level.INFO);							
							System.out.println("Response Received ::\n "+response);

							if(response.type == MessageType.READ)
							{
								String source = response.source;
								String key = response.msg;
								String val = response.val;
								if(readvalues.containsKey(source))
								{
									if(readvalues.get(source).containsKey(key)){
										System.out.println(key+" :: Expected value : "+readvalues.get(source).get(key)+", Obtained value :"+val+" \n");
										this.AddLogEntry(key+" :: Expected value "+readvalues.get(source).get(key)+", Obtained value "+val+"\n", Level.INFO);
										if(val != null){
											if(val.equals(readvalues.get(source).get(key)))
												readvalues.get(source).remove(key);
											else
												System.out.println("Miss count "+(++missCount));
											this.AddLogEntry("Miss count "+missCount, Level.INFO);
										}
										else{
											System.out.println("Miss count "+(++missCount));
											this.AddLogEntry("Miss count "+missCount, Level.INFO);
										}
									}
									
								}

							}
							Timestamp curtime =  new Timestamp(new Date().getTime());
							System.out.println("Time taken "+(curtime.getTime() - starttime.getTime()));
							this.AddLogEntry("Time taken "+(curtime.getTime() - starttime.getTime()), Level.INFO);
						}
						else{
							throw new NoSuchElementException();
						}
					}


				}
			} catch (ShutdownSignalException | ConsumerCancelledException
					| InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

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
						//	System.out.println("Read Set before addition "+readvalues.get(dest).entrySet());
						HashMap<String,String> temp = readvalues.get(dest);
						String value = str.nextToken();
						temp.put(message, value);
						readvalues.put(dest,temp);
						//	System.out.println("Adding to read set "+readvalues.get(dest).entrySet());
						this.sendReadMessage(transNo,message, dest+"Queue");
						break;
					case 3:	
						transNo = Integer.parseInt(str.nextToken());
						System.out.println("Txn No "+transNo);
						this.AddLogEntry("Txn No : "+transNo.toString(), Level.INFO);					
						message = str.nextToken();
						System.out.println("Object to write "+message);
						this.AddLogEntry("Object to write : "+message, Level.INFO);
						value = str.nextToken();
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
				Thread.sleep(10);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}




	public void finalize() throws IOException
	{

		channel.queueDelete(EXCHANGE_NAME+"Queue");
		channel.close();
		connection.close();
	}


	public static void main(String[] argv) throws Exception {

		if(argv.length != 3){
			System.out.println("Usage [NodeName] [Nodes in the system separated by comma] [FileName]");
			throw new IllegalArgumentException();
		}

		AutoClientReceiver obj = new AutoClientReceiver(argv[0],argv[2]);
		String[] allnodes = argv[1].split(",");
		obj.populateNodes(Arrays.asList(allnodes));

		obj.declareQueue();
		new Thread(obj).start();
		obj.sendMessages();

	}

}
