package messagefutures.node;


import messagefutures.common.*;
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
import messagefutures.message.*;

public class ClientReceiver implements Runnable {

	private String EXCHANGE_NAME = null;
	ConnectionFactory factory = new ConnectionFactory();
	private Connection connection ;
	private  Channel channel;
	private QueueingConsumer consumer ;
	private List<String> exchanges;
	public ClientReceiver(String exchName) throws IOException
	{
		this.EXCHANGE_NAME = exchName;
		factory = new ConnectionFactory();
		factory.setHost(Common.RMQServer);
		connection = factory.newConnection();
		channel = connection.createChannel();
		exchanges = new ArrayList<String>();
	}



	public void declareQueue() throws IOException
	{
		channel.queueDeclare(EXCHANGE_NAME+"Queue", false, false, false, null);
		consumer = new QueueingConsumer(channel);
		channel.basicConsume(EXCHANGE_NAME+"Queue", true, consumer);
	}

	public void populateNodes(List<String> list)
	{
		for(String node: list)
			exchanges.add(node);
	}
	
	public void run()
	{
		System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
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

							ClientResponse response = (ClientResponse) wrapper.getDeSerializedInnerMessage();
							System.out.println("Response Received :: \n "+response);
						}
						else{
							System.out.println("Client should not receive any other message");
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
			}

		}

	}
	
	
	

	public void finalize() throws IOException
	{

		channel.queueDelete(EXCHANGE_NAME+"Queue");
		channel.close();
		connection.close();
	}


	public static void main(String[] argv) throws Exception {

		if(argv.length != 2){
			System.out.println("Usage [NodeName] [Nodes in the system separated by comma]");
			throw new IllegalArgumentException();
		}

		ClientReceiver obj = new ClientReceiver(argv[0]);
		String[] allnodes = argv[1].split(",");
		obj.populateNodes(Arrays.asList(allnodes));

		obj.declareQueue();
		new Thread(obj).start();

	}

}