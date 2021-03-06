package messagefutures.node;

import messagefutures.event.*;
import messagefutures.common.*;
import messagefutures.message.*;
import messagefutures.bookkeep.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;



public class Datacentre implements Runnable {

	private static Configuration conf = null;
	private HashMap<Integer,TransactionDetail> txnDetail;
	private static int totalNodes;
	private HashMap<String,Integer> nodeMap ;
	private ArrayList<Integer> committedTxn;
	private ArrayList<Integer> pendingTxn;
	private ArrayList<Integer> activeTxn;
	private static Integer txnCount = 0;
	private static int ownIndex ;
	private static int[][] log;
	private String EXCHANGE_NAME = null;
	private ConnectionFactory factory = new ConnectionFactory();
	private Connection connection ;
	private  Channel channel;
	private QueueingConsumer consumer ;
	private List<String> exchanges;
	private HashMap<Integer,HashMap<Integer, HashMap<Integer,Event>>> logDetails;
	private Integer lastPropTime = 0;
	protected final static Logger LOGGER = Logger.getLogger(Datacentre.class.getName());
	private static FileHandler logFile;
	static Timestamp starttime;
	int timeout;
	static AtomicBoolean isLogModified ;
	static boolean autologProp = true;

	public Datacentre(String exchName) throws Exception
	{
		this.EXCHANGE_NAME = exchName;
		factory = new ConnectionFactory();
		factory.setHost(Common.RMQServer);
		connection = factory.newConnection();
		channel = connection.createChannel();
		channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
		nodeMap = new HashMap<String,Integer>();
		txnDetail = new HashMap<Integer,TransactionDetail>();
		pendingTxn = new ArrayList<Integer>();
		committedTxn = new ArrayList<Integer>();
		activeTxn = new ArrayList<Integer>();
		starttime = new Timestamp(new Date().getTime());
		conf = HBaseConfiguration.create();
		conf.set("hbase.master","localhost:9000");
		timeout = Common.timeout;
		isLogModified = new AtomicBoolean(false);
		File file = new File(Common.FilePath+"/"+this.EXCHANGE_NAME+".log");
		this.createLogFile(file);
		logFile = new FileHandler(file.getAbsolutePath(), true);
		logFile.setFormatter(new SimpleFormatter());
		LOGGER.setLevel(Level.INFO); 		
		LOGGER.addHandler(logFile);
		LOGGER.setUseParentHandlers(false);

		exchanges = new ArrayList<String>();
		this.createTable( Common.familys);
	}
	
	
	public void createLogFile(File file)
	{
		if(!file.exists())
		{
		  try
		  {
		    // Try creating the file
		    file.createNewFile();
		  }
		  catch(IOException ioe)
		  {
		    ioe.printStackTrace();
		  } 
		}
	}

	public void setTimeout(int timeout)
	{
		//seconds
		this.timeout = timeout;
	}

	private void unsetLogProp()
	{
		autologProp = false;
	}

	public void declareQueue(List<String> exchangeName) throws IOException
	{
		String queueName = channel.queueDeclare().getQueue();
		channel.queueDeclare(EXCHANGE_NAME+"Queue", false, false, false, null);	
		for(String exch: exchangeName){
			if(exch != null){
				channel.queueBind(queueName, exch, "");
			}
		}
		consumer = new QueueingConsumer(channel);
		channel.basicConsume(queueName, true, consumer);
		channel.basicConsume(EXCHANGE_NAME+"Queue", true, consumer);
	}




	public void generateNodeMap(List<String> list)
	{
		int count = 0;
		totalNodes = list.size();
		log = new int[totalNodes][totalNodes];
		for(int i =0;i< totalNodes; i++)
			Arrays.fill(log[i],-1);
		for(String temp : list){
			exchanges.add(temp);
			nodeMap.put(temp,count);
			if(temp.equals(EXCHANGE_NAME))
				ownIndex = count ;
			count++;
		}

		logDetails = new HashMap<Integer,HashMap<Integer,HashMap<Integer,Event>>>();
		for(int nodes = 0; nodes < totalNodes;nodes++)
		{
			HashMap<Integer,HashMap<Integer,Event>> temprow = new HashMap<Integer,HashMap<Integer,Event>>();
			for(int k =0;k< totalNodes;k++)
			{
				temprow.put(k, new HashMap<Integer,Event>());
			}
			logDetails.put(nodes,temprow);
		}


	}

	/**
	 * Create a table
	 */
	public void createTable(String[] familys)
			throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(Common.tableName)) {
			this.AddLogEntry("table already exists!",Level.INFO);
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(Common.tableName);
			for (int i = 0; i < familys.length; i++) {
				tableDesc.addFamily(new HColumnDescriptor(familys[i]));
			}
			admin.createTable(tableDesc);
			this.AddLogEntry("create table " + Common.tableName + " ok.",Level.INFO);
			admin.close();
		}
	}


	/**
	 * Put (or insert) a row
	 */
	public void addRecord(String rowKey, String value) throws Exception {
		try {
			HTable table = new HTable(conf, Common.tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(Common.familyName), Bytes.toBytes(Common.qualifier), Bytes
					.toBytes(value));
			table.put(put);
			System.out.println("insert recored " + rowKey + " to table "
					+ Common.tableName + " ok.");
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Put (or insert) a row
	 */
	public void updateRecord(String rowKey, String value) throws Exception {
		try {

			HTable table = new HTable(conf, Common.tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(Common.familyName), Bytes.toBytes(Common.qualifier), Bytes
					.toBytes(value));
			table.put(put);
			System.out.println("udpated recored " + rowKey + " to table "
					+ Common.tableName + " ok.");
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	/**
	 * Get a row
	 */
	public  void getOneRecord (String rowKey) throws IOException{
		Get get = new Get(rowKey.getBytes());
		HTable table = new HTable(conf, Common.tableName);
		Result rs = table.get(get);
		for(KeyValue kv : rs.raw()){
			System.out.print(new String(kv.getRow()) + " " );
			/*System.out.print(new String(kv.getFamily()) + ":" );
			System.out.print(new String(kv.getQualifier()) + " " );
			System.out.print(kv.getTimestamp() + " " );*/
			System.out.println(new String(kv.getValue()));
		}
		table.close();
	}


	/**
	 * Scan (or list) a table
	 */
	public void getAllRecord () {
		try{
			HTable table = new HTable(conf, Common.tableName);
			Scan s = new Scan();
			ResultScanner ss = table.getScanner(s);
			for(Result r:ss){
				for(KeyValue kv : r.raw()){
					System.out.print(new String(kv.getRow()) + " ");
					/*	System.out.print(new String(kv.getFamily()) + ":");
					System.out.print(new String(kv.getQualifier()) + " ");
					System.out.print(kv.getTimestamp() + " ");*/
					System.out.println(new String(kv.getValue()));
				}
			}
			table.close();
		} catch (IOException e){
			e.printStackTrace();
		}
	}




	public void writeRow(Integer txnNo, String rowKey, String value)
	{
		if(!txnDetail.containsKey(txnNo) ) return;
		else{
			TransactionDetail txndetail = txnDetail.get(txnNo);
			Record rec = txndetail.getRecord();
			HashMap<String,String> temp = rec.getWriteSet();						
			temp.put(rowKey, value);
			rec.setWriteSet(temp);						
			txndetail.setRecord(rec);
			txnDetail.put(txnNo,txndetail);
		}
	}


	public String readRow(Integer txnNo, String rowKey) throws IOException
	{

		if(!txnDetail.containsKey(txnNo) ) throw new NoSuchElementException();
		else{
			TransactionDetail txndetail = txnDetail.get(txnNo);
			Record rec = txndetail.getRecord();

			String result = "";
			HashMap<String,String> writeSet = rec.getWriteSet();
			HashMap<String,String> readSet = rec.getReadSet();



			if(writeSet.containsKey(rowKey))
			{
				if(writeSet.get(rowKey) != null){
					//System.out.println("Fetching from writeSet, Value "+writeSet.get(rowKey));
					result = writeSet.get(rowKey);
				}
			}
			else if(readSet.containsKey(rowKey))
			{
				if(readSet.get(rowKey)!= null){
					//System.out.println("Fetching from readSet, Value "+readSet.get(rowKey));
					result = readSet.get(rowKey);
				}
			}
			else{

				Get get = new Get(rowKey.getBytes());
				HTable table = new HTable(conf, Common.tableName);
				Result rs = table.get(get);
				HashMap<String,String> tablerecord = new HashMap<String,String>();
				for(KeyValue kv : rs.raw()){
					tablerecord.put(new String(kv.getQualifier()),new String(kv.getValue()));
				}
				table.close();

				//System.out.println("Adding to readSet , Value "+tablerecord.get(Common.qualifier));
				//System.out.println("Fetching from table ");
				result = tablerecord.get(Common.qualifier);				
				readSet.put(rowKey, result);
				rec.setReadSet(readSet);
				txndetail.setRecord(rec);
				txnDetail.put(txnNo, txndetail);
			}
			return result;
		}

	}


	public void printLog()
	{
		//System.out.println("Log :: "+totalNodes);
		for(int i=0;i<totalNodes;i++)
		{
			for(int j=0;j<totalNodes;j++){
				System.out.print(" "+log[i][j]);
			}
			System.out.println();
		}
	}


	public void commitTransaction(Integer txnNo) throws IOException
	{
		if(txnDetail.get(txnNo) != null)
		{
			//check if it is in pending trans list
			HashMap<String,String> temp = txnDetail.get(txnNo).getRecord().getWriteSet();
			HTable table = new HTable(conf, Common.tableName);
			for(String key : temp.keySet())
			{				
				Put put = new Put(Bytes.toBytes(key));				
				put.add(Bytes.toBytes(Common.familyName), Bytes.toBytes(Common.qualifier), Bytes
						.toBytes(temp.get(key)));				
				table.put(put);
			}
			table.close();
			pendingTxn.remove(pendingTxn.indexOf(txnNo));
			committedTxn.add(txnNo);
			CommitEvent commitEvent = new CommitEvent(txnNo, EventType.COMMIT,EXCHANGE_NAME, temp);
			System.out.println("Committing Transaction "+txnNo+" with writeSet "+temp.entrySet());
			this.AddLogEntry("Committing Transaction "+txnNo+" with writeSet "+temp.entrySet(), Level.INFO);
			HashMap<Integer,HashMap<Integer,Event>> temprow = logDetails.get(ownIndex);
			HashMap<Integer,Event> tempevent = temprow.get(ownIndex);
			tempevent.put(++log[ownIndex][ownIndex],commitEvent);
			temprow.put(ownIndex,tempevent);
			logDetails.put(ownIndex,temprow);
			isLogModified.getAndSet(true);
			if(autologProp){
				Timestamp curtime = new Timestamp(new Date().getTime());
				if(curtime.after(Common.getUpdatedTimestamp(starttime, timeout)))
				{
					this.sendLogMessage();
					starttime = curtime;
					isLogModified.getAndSet(false);
				}
			}
		}
		else{
			throw new NoSuchElementException();	
		}
	}



	public void commitWriteSet(String source,String transSource, HashMap<String,String> writeSet) throws IOException
	{
		if(writeSet == null) return;
		HTable table = new HTable(conf, Common.tableName);
		for(String key : writeSet.keySet())
		{				
			Put put = new Put(Bytes.toBytes(key));
			put.add(Bytes.toBytes(Common.familyName), Bytes.toBytes(Common.qualifier), Bytes
					.toBytes((writeSet.get(key))));
			table.put(put);
		}
		table.close();		
		txnDetail.put(txnCount,new TransactionDetail(source, txnCount));
		activeTxn.add(txnCount);	
		int txnNo = txnCount;
		txnCount++;	

		this.AddLogEntry("Replaying the commit from a different data centre "+writeSet.entrySet(), Level.INFO);		
		System.out.println("Replaying the commit from a different data centre "+writeSet.entrySet());
		activeTxn.remove(activeTxn.indexOf(txnNo));
		committedTxn.add(txnNo);
		CommitEvent commitEvent = new CommitEvent(txnNo, EventType.COMMIT,transSource, writeSet);

		HashMap<Integer,HashMap<Integer,Event>> temprow = logDetails.get(ownIndex);
		HashMap<Integer,Event> tempevent = temprow.get(ownIndex);
		tempevent.put(++log[ownIndex][ownIndex],commitEvent);
		temprow.put(ownIndex,tempevent);
		logDetails.put(ownIndex,temprow);
		isLogModified.getAndSet(true);
		if(autologProp){
			//System.out.println("Auto log prop is set : commit write set");
			Timestamp curtime = new Timestamp(new Date().getTime());
			if(curtime.after(Common.getUpdatedTimestamp(starttime, timeout)))
			{
				//System.out.println("Sending msg ");
				this.sendLogMessage();
				starttime = curtime;
				isLogModified.getAndSet(false);
			}
		}
	}

	public void abortTransaction(Integer txnNo) throws IOException
	{
		txnDetail.put(txnNo, null);
		txnDetail.remove(txnNo);
		//check if its in pending list on the first place
		if(activeTxn.contains(txnNo))
			activeTxn.remove(activeTxn.indexOf(txnNo));
		else if(pendingTxn.contains(txnNo))
			pendingTxn.remove(pendingTxn.indexOf(txnNo));		
		HashMap<Integer,HashMap<Integer,Event>> temprow = logDetails.get(ownIndex);
		HashMap<Integer,Event> tempevent = temprow.get(ownIndex);
		tempevent.put(++log[ownIndex][ownIndex],new AbortEvent(txnNo,EventType.ABORT,EXCHANGE_NAME));
		temprow.put(ownIndex,tempevent);
		logDetails.put(ownIndex,temprow);	
		this.AddLogEntry("Aborting Transaction "+txnNo, Level.INFO);
		System.out.println("Aborting Transaction "+txnNo);
		isLogModified.getAndSet(true);
		if(autologProp){
			Timestamp curtime = new Timestamp(new Date().getTime());
			if(curtime.after(Common.getUpdatedTimestamp(starttime, timeout)))
			{
				this.sendLogMessage();
				starttime = curtime;
				isLogModified.getAndSet(false);
			}
		}

	}


	public void listCommittedTxn()
	{
		System.out.println("Committed Transaction ");
		for(Integer txn : committedTxn)
			System.out.println("Txn No : "+txn);
	}

	public void listPendingTxn()
	{
		System.out.println("Pending Transaction ");
		for(Integer txn : pendingTxn)
			System.out.println("Txn No : "+txn);
	}

	public void listReadSet(Integer txnNo)
	{
		txnDetail.get(txnNo).getRecord().listReadSet();
	}

	public void listWriteSet(Integer txnNo)
	{
		txnDetail.get(txnNo).getRecord().listWriteSet();
	}



	/**
	 * Delete a table
	 */
	public void deleteTable() throws Exception {
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			admin.disableTable(Common.tableName);
			admin.deleteTable(Common.tableName);
			System.out.println("delete table " + Common.tableName + " ok.");
			admin.close();
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Delete a row
	 */
	public  void delRecord( String rowKey)
			throws IOException {
		HTable table = new HTable(conf, Common.tableName);
		List<Delete> list = new ArrayList<Delete>();
		Delete del = new Delete(rowKey.getBytes());
		list.add(del);
		table.delete(list);
		System.out.println("del recored " + rowKey + " ok.");
		table.close();
	}


	public void run()
	{
		System.out.println("Waiting for messages. To exit press CTRL+C");
		MessageWrapper wrapper ;
		QueueingConsumer.Delivery delivery;
		while (true) {			

			try {
				delivery = consumer.nextDelivery();
				if(delivery != null){

					String msg = new String(delivery.getBody());
					wrapper = MessageWrapper.getDeSerializedMessage(msg); 

					if (wrapper != null)
					{
						if (wrapper.getmessageclass() == ClientMessage.class )
						{

							ClientMessage request = (ClientMessage) wrapper.getDeSerializedInnerMessage();
							System.out.println();
							if(request.type.equals(MessageType.BEGIN)){
								System.out.println("BEGIN REQ ");
								this.handleBeginRequest(request);
							}
							else if(request.type.equals(MessageType.READ)){
								System.out.println("READ REQ ");
								this.handleReadRequest(request);
							}
							else if(request.type == MessageType.WRITE){
								System.out.println("WRITE REQ ");
								this.handleWriteRequest(request);
							}
							else if(request.type == MessageType.COMMIT){
								System.out.println("COMMIT REQ ");
								this.handleCommitRequest(request);
							}
							else if(request.type == MessageType.ABORT){
								System.out.println("ABORT REQ ");
								this.handleAbortRequest(request);
							}
							else if(request.type.equals(MessageType.SCAN)){
								System.out.println("SCAN REQ ");
								this.handleScanRequest();
							}
							else if(request.type == MessageType.LISTACTIVE)
								this.handleListActiveRequest(request);
							else if(request.type == MessageType.LISTPENDING)
								this.handleListPendingRequest(request);
							else if(request.type == MessageType.PROPLOG)
								this.sendLogMessage();

						}
						else if(wrapper.getmessageclass() == LogPropagationMesssage.class){
							LogPropagationMesssage logmessage = (LogPropagationMesssage)wrapper.getDeSerializedInnerMessage();
							//	System.out.println("Log Received from "+logmessage.source);
							this.handleLogPropMessage(logmessage);
						}
						else {
							throw new IllegalArgumentException();
						}


					}


					//String message = new String(delivery.getBody());

					//System.out.println(" [x] Received '" + message + "'");
				}
			} catch (ShutdownSignalException | ConsumerCancelledException
					| InterruptedException | ClassNotFoundException | IOException e) {
				e.printStackTrace();
			}


		}

	}

	public void handleBeginRequest(ClientMessage msg) throws IOException
	{
		this.AddLogEntry("Received Begin Request \n"+msg, Level.INFO);
		System.out.println("Received \n"+msg);
		txnDetail.put(txnCount,new TransactionDetail(msg.source, txnCount));
		activeTxn.add(txnCount);	
		ClientResponse responseMsg = new ClientResponse(EXCHANGE_NAME, MessageType.ACK, txnCount,"Transaction Created");
		txnCount++;
		this.sendClientResponse(msg.source+"Queue",responseMsg);
	}


	public void handleReadRequest(ClientMessage msg) throws IOException
	{
		this.AddLogEntry("Received Read Request \n"+msg, Level.INFO);
		System.out.println("Received \n"+msg);
		String key = msg.readObject;
		System.out.println("Txn No :"+msg.TransactionNo+ " key "+key);
		String val = this.readRow(msg.TransactionNo, key);
		ClientResponse response = new ClientResponse(EXCHANGE_NAME,MessageType.READ,msg.TransactionNo, key, val);
		this.sendClientResponse(msg.source+"Queue", response);
	}



	public void handleWriteRequest(ClientMessage msg) throws IOException
	{
		this.AddLogEntry("Received Write Request \n"+msg, Level.INFO);
		System.out.println("Received \n"+msg);
		String key = msg.writeObject;
		String val = msg.value;
		//System.out.println(" write req "+msg.TransactionNo+" key "+key+" val "+val);
		this.writeRow(msg.TransactionNo, key, val);
	}


	public void handleCommitRequest(ClientMessage msg) throws IOException {

		this.AddLogEntry("Received Commit Request \n"+msg, Level.INFO);
		System.out.println("Received Commit Reqeust \n"+msg);
		Integer txnNo = msg.TransactionNo;
		/*for(int acttemp : activeTxn)
			System.out.print(" "+acttemp);*/

		if(activeTxn.contains(txnNo)){
			activeTxn.remove(activeTxn.indexOf(txnNo));
			if(checktoQualifyPending(txnDetail.get(txnNo).getRecord().getWriteSet()))
			{
				pendingTxn.add(txnNo);

				HashMap<Integer,HashMap<Integer,Event>> temprow = logDetails.get(ownIndex);

				HashMap<Integer,Event> tempevent = temprow.get(ownIndex);
				log[ownIndex][ownIndex] += 1;
				tempevent.put(log[ownIndex][ownIndex], new PendingEvent(txnNo, EventType.PENDING ,EXCHANGE_NAME,txnDetail.get(txnNo).getRecord().getWriteSet()));

				temprow.put(ownIndex,tempevent);
				logDetails.put(ownIndex,temprow);	

				//	System.out.println("  new value "+log[ownIndex][ownIndex]);
				TransactionDetail txndet = txnDetail.get(txnNo); 
				txndet.setLastPropTime(lastPropTime);
				txnDetail.put(txnNo, txndet);
				isLogModified.getAndSet(true);
				if(autologProp){
					Timestamp curtime = new Timestamp(new Date().getTime());
					if(curtime.after(Common.getUpdatedTimestamp(starttime, timeout)))
					{
						this.sendLogMessage();
						starttime = curtime;
						isLogModified.getAndSet(false);
					}
				}
				checktoCommit();
			}
			else{
				abortTransaction(txnNo);
			}
		}
		else{
			throw new NoSuchElementException();
		}
	}

	public void handleAbortRequest(ClientMessage msg) throws IOException
	{
		this.AddLogEntry("Received Abort Request \n"+msg, Level.INFO);
		System.out.println("Received Abort Request \n"+msg);
		this.abortTransaction(msg.TransactionNo);
		ClientResponse response = new ClientResponse(EXCHANGE_NAME, MessageType.ABORTACK, msg.TransactionNo,"Aborted");
		this.sendClientResponse(msg.source+"Queue", response);
	}


	public void handleScanRequest()
	{
		this.AddLogEntry("Received Scan Request ", Level.INFO);
		System.out.println("Received Scan Request \n");
		this.getAllRecord();
	}

	public void handleListActiveRequest(ClientMessage msg) throws IOException
	{
		this.AddLogEntry("Received ListActive Request "+msg, Level.INFO);
		System.out.println("Received ListPending Request \n"+msg);
		StringBuffer buffer = new StringBuffer();
		buffer.append(" total "+activeTxn.size()+" \n");
		for(int temp:activeTxn)
			buffer.append(" "+ temp);
		ClientResponse response = new ClientResponse(EXCHANGE_NAME, MessageType.LISTACTIVE, new String(buffer));
		this.sendClientResponse(msg.source+"Queue", response);
	}

	public void handleListPendingRequest(ClientMessage msg) throws IOException
	{
		this.AddLogEntry("Received ListPending Request \n"+msg, Level.INFO);
		System.out.println("Received ListPending Request \n"+msg);
		StringBuffer buffer = new StringBuffer();
		buffer.append(" total "+pendingTxn.size()+" \n");
		for(int temp:pendingTxn)
			buffer.append(" "+ temp);
		ClientResponse response = new ClientResponse(EXCHANGE_NAME, MessageType.LISTPENDING, new String(buffer));
		this.sendClientResponse(msg.source+"Queue", response);
	}


	public void handleLogPropMessage(LogPropagationMesssage msg) throws IOException
	{
		this.AddLogEntry("Received Log from "+msg.source+"\n"+msg, Level.INFO);
		System.out.println("Received Log from "+msg.source+"\n"+msg);
		checkConflicts(msg.source, msg.log, msg.logDetails);
	}


	public boolean checktoQualifyPending(HashMap<String,String> writeSet)
	{
		for(int k=0;k<pendingTxn.size();k++)
		{
			if(ifConflict(txnDetail.get(pendingTxn.get(k)).getRecord(),writeSet))
			{
				return false;										
			}
		}
		return true;
	}


	public void checkConflicts(String source, int[][] inLog,HashMap<Integer,HashMap<Integer, HashMap<Integer, Event>>> inLogDetails) throws IOException
	{
		int count = 0;
		int inIndex = 0;
		for(String temp : exchanges){
			if(temp.equals(source))
				inIndex = count;
			count++;
		}

		ArrayList<Event> incomingEvents=new ArrayList<Event>();
		HashSet<Integer> toAbort = new HashSet<Integer>();

		for(int i = 0;i < totalNodes; i++)
		{

			for(int j = 0;j < totalNodes; j++)
			{
				if(i != ownIndex)
				{
					if(j != ownIndex){

						if(log[i][j] < inLog[i][j])
						{
							if(i == j){
								incomingEvents = new ArrayList<Event>();
								for(int eventno = log[i][j]+1; eventno <= inLog[i][j]; eventno++)
								{									
									toAbort = new HashSet<Integer>();
									if(inLogDetails.get(j).get(j).get(eventno).getEventType() == EventType.PENDING){
										isLogModified.getAndSet(true);
										HashMap<String,String> writeset = inLogDetails.get(j).get(j).get(eventno).getWriteSet();
										for(int k=0;k<pendingTxn.size();k++)
										{
											if(ifConflict(txnDetail.get(pendingTxn.get(k)).getRecord(),writeset))
											{
												toAbort.add(pendingTxn.get(k));										
											}
										}
									}
									
									if(inLogDetails.get(j).get(j).get(eventno).getEventType() == EventType.COMMIT ){

										if(!inLogDetails.get(j).get(j).get(eventno).getTransScource().equals(EXCHANGE_NAME)){
											isLogModified.getAndSet(true);
											HashMap<String,String> writeset = inLogDetails.get(j).get(j).get(eventno).getWriteSet();
											for(int k=0;k<pendingTxn.size();k++)
											{
												if(ifConflict(txnDetail.get(pendingTxn.get(k)).getRecord(),writeset))
												{
													toAbort.add(pendingTxn.get(k));										
												}
											}
										}
									}
									
									if(inLogDetails.get(j).get(j).get(eventno).getEventType() == EventType.COMMIT )
									{
										System.out.println("Origin of Transaction : "+inLogDetails.get(j).get(j).get(eventno).getTransScource());
										if(!inLogDetails.get(j).get(j).get(eventno).getTransScource().equals(EXCHANGE_NAME)){
											//isLogModified.getAndSet(true);
											commitWriteSet(source,inLogDetails.get(j).get(j).get(eventno).getTransScource(), inLogDetails.get(j).get(j).get(eventno).getWriteSet());
										}
										else{
											System.out.println("Not replaying as the Transaction originated in same DataCentre : "+inLogDetails.get(j).get(j).get(eventno).getWriteSet());
											this.AddLogEntry("Not replaying as the Transaction originated in same DataCentre "+inLogDetails.get(j).get(j).get(eventno).getWriteSet(), Level.INFO);
										}
									}

									log[i][j]++;								

									HashMap<Integer,HashMap<Integer,Event>> temprow = logDetails.get(i);
									HashMap<Integer,Event> tempevent = temprow.get(i);
									tempevent.put(eventno, inLogDetails.get(i).get(i).get(eventno));
									temprow.put(i,tempevent);
									logDetails.put(i,temprow);
									log[ownIndex][j] = log[i][j];
									//abort all trans in toAbort
									if(toAbort.size() > 0){
										System.out.println("Aborting due to conflicts");
										this.AddLogEntry("Aborting due to conflicts", Level.INFO);
										for(Integer aborttxnno: toAbort)
										{
											//System.out.println("Going to abort "+aborttxnno);
											abortTransaction(aborttxnno);
											
										}
									}
								}
								if(isLogModified.get())
								{	
									if(autologProp){
										Timestamp curtime = new Timestamp(new Date().getTime());
										if(curtime.after(Common.getUpdatedTimestamp(starttime, timeout)))
										{
											this.sendLogMessage();
											starttime = curtime;
										}
									}
									isLogModified.getAndSet(false);
								}
							}
							else{
								log[i][j] = inLog[i][j];
							}
						}
					}
					else
					{
						log[i][j] = inLog[i][j];						
					}
				}


			}

		}


		//update table
		for(int i =0;i<totalNodes;i++)
		{
			for(int j=0;j<totalNodes;j++){
				if(i != ownIndex)
				{
					if(i == j)
					{
						if(log[ownIndex][j]< log[i][j])
							log[ownIndex][j] = log[i][j];
					}
				}
			}
		}

		checktoCommit();
	}


	public void checktoCommit() throws IOException
	{
		//System.out.println("Checking if we can commit any transaction");
		for(Integer val:pendingTxn)
		{
			Integer lptEvent = txnDetail.get(val).getLastPropTime();
			//	System.out.println(" Last prop event for "+val +"  = "+lptEvent);
			//System.out.println("Own Index "+ownIndex);
			boolean flag = true;
			for(int i=0;i<totalNodes;i++)
			{
				if(i != ownIndex)
				{
					if(log[i][ownIndex]<lptEvent)
					{
						flag = false;
						break;
					}
				}
			}
			if(flag)
			{
				//System.out.println("Committing Txn no :: "+val);
				commitTransaction(val);
				//System.out.println(" Source  "+txnDetail.get(val).getClientIP());
				ClientResponse resp = new ClientResponse(EXCHANGE_NAME, MessageType.COMMITACK, val,"Write set "+txnDetail.get(val).getRecord().getWriteSet().entrySet());
				sendClientResponse(txnDetail.get(val).getClientIP()+"Queue", resp);
				break;
			}
		}


	}
	public boolean ifConflict(Record record, HashMap<String,String> writeSet)
	{
		if(record == null || writeSet == null) return true;
		boolean ifconflict = false;
		for(String temp : writeSet.keySet())
		{
			if(record.getReadSet().containsKey(temp))
			{
				ifconflict = true;
				break;				
			}

			if(record.getWriteSet().containsKey(temp))
			{
				ifconflict = true;
				break;
			}
		}

		return ifconflict;
	}


	public void AddLogEntry(String message, Level level){		
		LOGGER.logp(level, this.getClass().toString(), "", message);		
	}

	public void sendClientResponse(String dest,ClientResponse msg) throws IOException
	{
		this.AddLogEntry("Sent " + msg,Level.INFO);
		System.out.println("Sent " +msg);
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		channel.basicPublish("",dest, null, msgwrap.getSerializedMessage().getBytes());
	}

	public void sendMsgtoClient(String msg) throws IOException
	{
		this.AddLogEntry("Sent " + msg,Level.INFO);
		System.out.println("Sent " +msg);
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		channel.basicPublish("","c1Queue", null, msgwrap.getSerializedMessage().getBytes());		
	}

	public void sendMsgs() throws NumberFormatException, IOException
	{
		Integer option = null;
		do{
			System.out.println("1 - Propagate Log, 2- list log, 3 - send msg to client, 4 - exit");
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			option = Integer.parseInt(br.readLine());
			if(option != 4)
			{
				switch(option){
				case 1:					
					this.sendLogMessage();
					break;
				case 2:
					this.printLog();
					break;
				case 3:
					System.out.println("Enter the msg ");
					String message = br.readLine();
					this.sendMsgtoClient(message);
					break;
				default:
					System.out.println("Enter correct Choice");
					break;
				}
			}
		}while(true);

	}



	public void sendLogMessage() throws IOException
	{
		lastPropTime = ++log[ownIndex][ownIndex];			
		HashMap<Integer,HashMap<Integer,Event>> temprow = logDetails.get(ownIndex);		
		HashMap<Integer,Event> tempevent = temprow.get(ownIndex);
		tempevent.put(log[ownIndex][ownIndex], new LPTEvent(EventType.LPT,EXCHANGE_NAME));
		temprow.put(ownIndex,tempevent);
		logDetails.put(ownIndex,temprow);	
		LogPropagationMesssage msg = new LogPropagationMesssage(EXCHANGE_NAME,logDetails,log);
		this.AddLogEntry("Sent Log Message \n"+msg, Level.INFO);
		System.out.println("Sent Log Message " + msg );
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		channel.basicPublish(EXCHANGE_NAME, "", null, msgwrap.getSerializedMessage().getBytes());
	} 


	public void finalize() throws IOException
	{

		for(String exch: exchanges)
			channel.exchangeDelete(exch);
		channel.queueDelete(EXCHANGE_NAME+"Queue");
		channel.close();
		connection.close();
	}


	public static void main(String[] args) throws Exception {

		if(args.length < 3){
			System.out.println("Usage [NodeName] [Nodes in the system separated by comma] [Node to which current node is connected to] [Log Propagation Time] ");
			throw new IllegalArgumentException();
		}

		Datacentre obj = new Datacentre(args[0]);
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		System.out.println("Subscribe to exchanges  ");

		br.readLine();
		String[] exchlist = args[2].split(",");

		obj.declareQueue(Arrays.asList(exchlist));
		String[] allNodes = args[1].split(",");
		obj.generateNodeMap(Arrays.asList(allNodes));
		if(args.length == 4){
			if(!args[3].equals("0"))	{		
				obj.setTimeout(Integer.parseInt(args[3]));
				new Thread(new LogPropagation(obj)).start();
			}
			else
				obj.unsetLogProp();
		}
		else{
			new Thread(new LogPropagation(obj)).start();
		}
		new Thread(obj).start();
		obj.sendMsgs();
	}
}


class LogPropagation implements Runnable{

	private Datacentre object;
	public LogPropagation(Datacentre obj) {
		this.object = obj;
	}

	public void run()
	{
		while(true)
		{
			//System.out.println("Checking to prop de log ");
			if(object.autologProp){
				//System.out.println("Auto prop is set ");
			if(object.isLogModified.get())
			{	
				//System.out.println("Log is modified ");
				
				
					Timestamp curtime = new Timestamp(new Date().getTime());
					if(curtime.after(Common.getUpdatedTimestamp(object.starttime, Common.timeout)))
					{
						//System.out.println("time elapsed");
						try {
							object.sendLogMessage();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						object.starttime = curtime;
						object.isLogModified.getAndSet(false);
					}
				}
			}
			try {
				Thread.sleep(object.timeout*1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}


