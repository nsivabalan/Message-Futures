package messagefutures.node;

import messagefutures.event.*;
import messagefutures.common.*;
import messagefutures.message.*;
import messagefutures.bookkeep.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

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


	public Datacentre(String exchName) throws Exception
	{
		this.EXCHANGE_NAME = exchName;
		factory = new ConnectionFactory();
		factory.setHost("localhost");
		connection = factory.newConnection();
		channel = connection.createChannel();
		channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
		nodeMap = new HashMap<String,Integer>();
		txnDetail = new HashMap<Integer,TransactionDetail>();
		pendingTxn = new ArrayList<Integer>();
		committedTxn = new ArrayList<Integer>();
		activeTxn = new ArrayList<Integer>();
		conf = HBaseConfiguration.create();
		conf.set("hbase.master","localhost:9000");
		exchanges = new ArrayList<String>();
		this.createTable( Common.familys);



	}

	public void declareQueue(List<String> exchangeName) throws IOException
	{
		String queueName = channel.queueDeclare().getQueue();
		channel.queueDeclare(EXCHANGE_NAME+"Queue", false, false, false, null);
		System.out.println("QueueName "+queueName);
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
			System.out.println("table already exists!");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(Common.tableName);
			for (int i = 0; i < familys.length; i++) {
				tableDesc.addFamily(new HColumnDescriptor(familys[i]));
			}
			admin.createTable(tableDesc);
			System.out.println("create table " + Common.tableName + " ok.");
			admin.close();
		}
	}


	/**
	 * Put (or insert) a row
	 */
	public void addRecord(String rowKey, String qualifier, String value) throws Exception {
		try {
			HTable table = new HTable(conf, Common.tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(Common.familyName), Bytes.toBytes(qualifier), Bytes
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
	public void updateRecord(String rowKey,String qualifier, String value) throws Exception {
		try {

			HTable table = new HTable(conf, Common.tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(Common.familyName), Bytes.toBytes(qualifier), Bytes
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
			System.out.print(new String(kv.getFamily()) + ":" );
			System.out.print(new String(kv.getQualifier()) + " " );
			System.out.print(kv.getTimestamp() + " " );
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
					System.out.print(new String(kv.getFamily()) + ":");
					System.out.print(new String(kv.getQualifier()) + " ");
					System.out.print(kv.getTimestamp() + " ");
					System.out.println(new String(kv.getValue()));
				}
			}
			table.close();
		} catch (IOException e){
			e.printStackTrace();
		}
	}




	public void writeRow(Integer txnNo, String rowKey, String colQualifier, String value)
	{
		if(!txnDetail.containsKey(txnNo) ) return;
		else{
			TransactionDetail txndetail = txnDetail.get(txnNo);
			Record rec = txndetail.getRecord();
			HashMap<String,HashMap<String,String>> temp = rec.getWriteSet();
			if(temp.containsKey(rowKey))
			{
				System.out.println(" write set contains "+rowKey);
				HashMap<String,String> colval = temp.get(rowKey);	
				if(colval == null){
					colval = new HashMap<String,String>();
					System.out.println(" colval is null ");
				}
				colval.put(colQualifier, value);
				temp.put(rowKey, colval);
				rec.setWriteSet(temp);			
			}
			else{			
				System.out.println("write set doesnt contain "+rowKey);
				HashMap<String,String> colval = new HashMap<String,String>();						
				colval.put(colQualifier, value);
				temp.put(rowKey,colval);
				rec.setWriteSet(temp);
			}
			txndetail.setRecord(rec);
			txnDetail.put(txnNo,txndetail);
		}
	}


	public String readRow(Integer txnNo, String rowKey, String colQualifier) throws IOException
	{

		if(!txnDetail.containsKey(txnNo) ) return "";
		else{
			TransactionDetail txndetail = txnDetail.get(txnNo);
			Record rec = txndetail.getRecord();

			String result = "";
			HashMap<String,HashMap<String,String>> writeSet = rec.getWriteSet();
			HashMap<String,HashMap<String,String>> readSet = rec.getReadSet();

			Get get = new Get(rowKey.getBytes());
			HTable table = new HTable(conf, Common.tableName);
			Result rs = table.get(get);
			HashMap<String,String> tablerecord = new HashMap<String,String>();
			for(KeyValue kv : rs.raw()){
				tablerecord.put(new String(kv.getQualifier()),new String(kv.getValue()));
			}
			table.close();

			if(writeSet.containsKey(rowKey))
			{
				if(writeSet.get(rowKey).get(colQualifier) != null){
					System.out.println("Fetching from writeSet Qualifier "+colQualifier+", Value "+writeSet.get(rowKey).get(colQualifier));
					result = writeSet.get(rowKey).get(colQualifier);
				}
			}
			else if(readSet.containsKey(rowKey))
			{
				if(readSet.get(rowKey).get(colQualifier) != null){
					System.out.println("Fetching from readSet Qualifier "+colQualifier+", Value "+readSet.get(rowKey).get(colQualifier));
					result = readSet.get(rowKey).get(colQualifier);
				}
			}
			else{
				System.out.println("Adding to readSet Qualifier "+colQualifier+", Value "+tablerecord.get(colQualifier));
				System.out.println("Fetching from table ");
				result = tablerecord.get(colQualifier);
				HashMap<String,String> temp = readSet.get(rowKey);
				if(temp == null)
					temp = new HashMap<String,String>();
				temp.put(colQualifier, tablerecord.get(colQualifier));
				readSet.put(rowKey, temp);
				rec.setReadSet(readSet);
				txndetail.setRecord(rec);
				txnDetail.put(txnNo, txndetail);
			}
			return result;
		}

	}


	public void printLog()
	{
		System.out.println("Log :: "+totalNodes);
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
			//check if its in pending trans list
			HashMap<String,HashMap<String,String>> temp = txnDetail.get(txnNo).getRecord().getWriteSet();
			HTable table = new HTable(conf, Common.tableName);
			for(String key : temp.keySet())
			{
				HashMap<String,String> colvaltxn = temp.get(key);				
				Put put = new Put(Bytes.toBytes(key));
				for(String col: colvaltxn.keySet()){
					put.add(Bytes.toBytes(Common.familyName), Bytes.toBytes(col), Bytes
							.toBytes(colvaltxn.get(col)));
				}
				table.put(put);
			}
			table.close();
			pendingTxn.remove(pendingTxn.indexOf(txnNo));
			committedTxn.add(txnNo);
			CommitEvent commitEvent = new CommitEvent(txnNo, EventType.COMMIT, temp);

			HashMap<Integer,HashMap<Integer,Event>> temprow = logDetails.get(ownIndex);
			HashMap<Integer,Event> tempevent = temprow.get(ownIndex);
			tempevent.put(++log[ownIndex][ownIndex],commitEvent);
			temprow.put(ownIndex,tempevent);
			logDetails.put(ownIndex,temprow);			
			//log[ownIndex][ownIndex]++;

		}
		else{
			System.out.println("No Such Transaction exists ");			
		}
	}

	public void abortTransaction(Integer txnNo)
	{
		txnDetail.put(txnNo, null);
		txnDetail.remove(txnNo);
		//check if its in pending list on the first place
		if(activeTxn.contains(txnNo))
			activeTxn.remove(activeTxn.indexOf(txnNo));
		else
			pendingTxn.remove(pendingTxn.indexOf(txnNo));		
		HashMap<Integer,HashMap<Integer,Event>> temprow = logDetails.get(ownIndex);
		HashMap<Integer,Event> tempevent = temprow.get(ownIndex);
		tempevent.put(++log[ownIndex][ownIndex],new AbortEvent(txnNo,EventType.ABORT));
		temprow.put(ownIndex,tempevent);
		logDetails.put(ownIndex,temprow);	
		System.out.println("Transaction with trans No "+txnNo+" removed from pending transaction list");

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
		System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
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
						if (wrapper.getmessageclass() == ClientOpMessage.class )
						{

							ClientOpMessage request = (ClientOpMessage) wrapper.getDeSerializedInnerMessage();
							System.out.println("Request Received :: \n "+request);
							System.out.println("type :::::::: "+request.type);
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

						}
						else if(wrapper.getmessageclass() == LogPropagationMesssage.class){
							LogPropagationMesssage logmessage = (LogPropagationMesssage)wrapper.getDeSerializedInnerMessage();
							System.out.println("Log Received from "+logmessage.source);
							this.handleLogPropMessage(logmessage);
						}
						else {
							System.out.println("Node should not receive any other messages ");
						}
					}


					String message = new String(delivery.getBody());

					System.out.println(" [x] Received '" + message + "'");
				}
			} catch (ShutdownSignalException | ConsumerCancelledException
					| InterruptedException | ClassNotFoundException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	public void handleBeginRequest(ClientOpMessage msg) throws IOException
	{
		txnDetail.put(txnCount,new TransactionDetail(msg.source, txnCount));
		activeTxn.add(txnCount);	
		ClientResponse responseMsg = new ClientResponse(EXCHANGE_NAME, MessageType.ACK, txnCount,"Transaction Created");
		txnCount++;
		this.sendClientResponse(msg.source+"Queue",responseMsg);
	}


	public void handleReadRequest(ClientOpMessage msg) throws IOException
	{
		String key = msg.readObject;
		String qualifier = msg.qualifier;
		System.out.println("Txn No :"+msg.TransactionNo+ " key "+key+" qual "+qualifier);
		String val = this.readRow(msg.TransactionNo, key, qualifier);
		ClientResponse response = new ClientResponse(EXCHANGE_NAME,MessageType.READ,msg.TransactionNo, val);
		this.sendClientResponse(msg.source+"Queue", response);
	}



	public void handleWriteRequest(ClientOpMessage msg) throws IOException
	{
		String key = msg.writeObject;
		String qual = msg.qualifier;
		String val = msg.value;
		System.out.println(" write req "+msg.TransactionNo+" key "+key+" qual "+qual+" val "+val);
		this.writeRow(msg.TransactionNo, key,qual, val);
	}



	public void handleCommitRequest(ClientOpMessage msg) throws IOException {

		Integer txnNo = msg.TransactionNo;
		for(int acttemp : activeTxn)
			System.out.print(" "+acttemp);

		if(activeTxn.contains(txnNo)){
			activeTxn.remove(activeTxn.indexOf(txnNo));
			pendingTxn.add(txnNo);

			HashMap<Integer,HashMap<Integer,Event>> temprow = logDetails.get(ownIndex);

			HashMap<Integer,Event> tempevent = temprow.get(ownIndex);
			tempevent.put(++log[ownIndex][ownIndex], new PendingEvent(txnNo, EventType.PENDING ,txnDetail.get(txnNo).getRecord().getWriteSet()));

			temprow.put(ownIndex,tempevent);
			logDetails.put(ownIndex,temprow);	

		//	System.out.println("  new value "+log[ownIndex][ownIndex]);
			TransactionDetail txndet = txnDetail.get(txnNo); 
			txndet.setLastPropTime(lastPropTime);
			txnDetail.put(txnNo, txndet);
			checktoCommit();
		}
		else{
			System.out.println("No such Active Transaction ");
		}
	}

	public void handleAbortRequest(ClientOpMessage msg) throws IOException
	{
		this.abortTransaction(msg.TransactionNo);
		ClientResponse response = new ClientResponse(EXCHANGE_NAME, MessageType.ABORTACK, msg.TransactionNo,"Aborted");
		this.sendClientResponse(msg.source+"Queue", response);
	}


	public void handleScanRequest()
	{
		this.getAllRecord();
	}

	public void handleListActiveRequest(ClientOpMessage msg) throws IOException
	{

		StringBuffer buffer = new StringBuffer();
		buffer.append(" total "+activeTxn.size()+" \n");
		for(int temp:activeTxn)
			buffer.append(" "+ temp);
		ClientResponse response = new ClientResponse(EXCHANGE_NAME, MessageType.LISTACTIVE, new String(buffer));
		this.sendClientResponse(msg.source+"Queue", response);
	}

	public void handleListPendingRequest(ClientOpMessage msg) throws IOException
	{
		StringBuffer buffer = new StringBuffer();
		buffer.append(" total "+pendingTxn.size()+" \n");
		for(int temp:pendingTxn)
			buffer.append(" "+ temp);
		ClientResponse response = new ClientResponse(EXCHANGE_NAME, MessageType.LISTPENDING, new String(buffer));
		this.sendClientResponse(msg.source+"Queue", response);
	}


	public void handleLogPropMessage(LogPropagationMesssage msg) throws IOException
	{
		checkConflicts(msg.source, msg.log, msg.logDetails);
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
									System.out.println("Processing event "+i+" "+j+" "+eventno);
									toAbort = new HashSet<Integer>();
									if(inLogDetails.get(j).get(j).get(eventno).getEventType() == EventType.COMMIT || inLogDetails.get(j).get(j).get(eventno).getEventType() == EventType.PENDING){
									HashMap<String,HashMap<String,String>> writeset = inLogDetails.get(j).get(j).get(eventno).getWriteSet();
									for(int k=0;k<pendingTxn.size();k++)
									{
										if(ifConflict(txnDetail.get(pendingTxn.get(k)).getRecord(),writeset))
										{
											toAbort.add(pendingTxn.get(k));										
										}
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
									for(Integer aborttxnno: toAbort)
									{
										System.out.println("Going to abort "+aborttxnno);
										abortTransaction(aborttxnno);
									}

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
		System.out.println("Checking if we can commit any transaction");
		for(Integer val:pendingTxn)
		{
			Integer lptEvent = txnDetail.get(val).getLastPropTime();
			System.out.println(" Last prop event for "+val +"  = "+lptEvent);
			System.out.println("Own Index "+ownIndex);
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
				System.out.println("Committing Txn no :: "+val);
				commitTransaction(val);
				System.out.println(" Source  "+txnDetail.get(val).getClientIP());
				ClientResponse resp = new ClientResponse(EXCHANGE_NAME, MessageType.COMMITACK, val);
				sendClientResponse(txnDetail.get(val).getClientIP()+"QUEUE", resp);
				break;
			}
		}

		
	}
	public boolean ifConflict(Record record, HashMap<String,HashMap<String,String>> writeSet)
	{
		if(record == null || writeSet == null) return true;
		boolean ifconflict = false;
		for(String temp : writeSet.keySet())
		{
			if(record.getReadSet().containsKey(temp))
			{
				HashMap<String,String> writeset = writeSet.get(temp);
				HashMap<String,String> rec = record.getReadSet().get(temp);
				for(String qual : writeset.keySet())
				{
					if(rec.containsKey(qual))
						ifconflict = true;
					break;
				}
			}

			if(record.getWriteSet().containsKey(temp))
			{
				HashMap<String,String> writeset = writeSet.get(temp);
				HashMap<String,String> rec = record.getWriteSet().get(temp);
				for(String qual : writeset.keySet())
				{
					if(rec.containsKey(qual))
						ifconflict = true;
					break;
				}
			}

		}

		return ifconflict;
	}



	public void sendClientResponse(String dest,ClientResponse msg) throws IOException
	{
		System.out.println(" [x] Sent '" + msg + "'");
		MessageWrapper msgwrap = new MessageWrapper(Common.Serialize(msg), msg.getClass());
		System.out.println("Destination -- ----- "+dest);
		channel.basicPublish("",dest, null, msgwrap.getSerializedMessage().getBytes());
	}

	public void sendMsgs() throws NumberFormatException, IOException
	{
		Integer option = null;
		do{
			System.out.println("1 - Send msg, 2- list log, 3 - commit trans, 4 - abort trans, 5 - exit");
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			option = Integer.parseInt(br.readLine());
			if(option != 5)
			{
				switch(option){
				case 1:
					System.out.println("Enter the msg ");
					String message = br.readLine();
					this.sendLogMessage();
					break;
				case 2:
					this.printLog();
					break;
				case 3:
					break;
				case 4:
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
		tempevent.put(log[ownIndex][ownIndex], new LPTEvent(EventType.LPT));
		temprow.put(ownIndex,tempevent);
		logDetails.put(ownIndex,temprow);	
		LogPropagationMesssage msg = new LogPropagationMesssage(EXCHANGE_NAME,logDetails,log);
		System.out.println(" [x] Sent Log Message '" + msg + "'");
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


		if(args.length != 3){
			System.out.println("Usage [NodeName] [Nodes in the system separated by comma] [Node to which current node is connected to]");
			throw new IllegalArgumentException();
		}

		Datacentre obj = new Datacentre(args[0]);
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		System.out.println("Subsribe to exchanges  ");

		String option = br.readLine();
		String[] exchlist = args[2].split(",");

		obj.declareQueue(Arrays.asList(exchlist));
		String[] allNodes = args[1].split(",");
		obj.generateNodeMap(Arrays.asList(allNodes));

		new Thread(obj).start();
		obj.sendMsgs();

	}
}
