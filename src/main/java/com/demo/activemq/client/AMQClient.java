package com.demo.activemq.client;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSslConnectionFactory;

public class AMQClient {

	public static void main(String[] args) {
		 AMQClient client=new AMQClient();
/*		 for (int i=0; i < 10 ; i++) {
			 client.send(" <order><orderId>1</orderId><orderItems>	<orderItemId>1</orderItemId>	<orderItemQty>1</orderItemQty>	<orderItemPublisherName>Orly</orderItemPublisherName>	<orderItemPrice>10.59</orderItemPrice></orderItems></order> ");
			//client.send(" <order><orderId>1</orderId><orderItems>	<orderItemId>1</orderItemId>	<orderItemQty>1</orderItemQty>	<orderItemPublisherName>ABC Company</orderItemPublisherName>	<orderItemPrice>10.59</orderItemPrice></orderItems></order> ");
			try {
				Thread.sleep(1500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		 }*/
		 client.consume("demo.queue.1");
/*
 <order><orderId>1</orderId><orderItems>	<orderItemId>1</orderItemId>	<orderItemQty>1</orderItemQty>	<orderItemPublisherName>Orly</orderItemPublisherName>	<orderItemPrice>10.59</orderItemPrice></orderItems></order> 
  */
		 //client.consume("demo.queue.1");
	}
	
//	private ActiveMQConnectionFactory connectionFactory;
	private ActiveMQConnectionFactory connectionFactory;
	private String url="ssl://broker-amq-tcp-ssl-amq-demo.192.168.223.196.nip.io:443";
	//for nodeport, in aws, point to any node except master as the SG is not setup there
//	/tcp://54.255.166.167:30002
	private Connection connection;
	//private String queue="demo.queue.1";
	private String queue="demo.queue.1";
	public AMQClient() {
		this(false);
	}
	public AMQClient(String host, String user, String password) {
		
	}
	
	public AMQClient(boolean ssl) {
		if (ssl) {
			try {
				//ocp
				connectionFactory = (ActiveMQSslConnectionFactory)new ActiveMQSslConnectionFactory(url);
				//connectionFactory = new ActiveMQSslConnectionFactory("ssl://localhost:61617");
				((ActiveMQSslConnectionFactory)connectionFactory).setTrustStore("file:/home/virtuser/keystores/demo/client.ts");
				((ActiveMQSslConnectionFactory)connectionFactory).setTrustStorePassword("password");
			} catch (Exception e) {
				e.printStackTrace();
			}			
		}
		else {
			connectionFactory=new ActiveMQConnectionFactory("tcp://192.168.223.196:30003");
			System.out.println("connectionFactory "+connectionFactory.toString());
			
		}
		connectionFactory.setUserName("admin");
		connectionFactory.setPassword("admin");		
		//connectionFactory.setUserName("joe");
		//connectionFactory.setPassword("password");		
	}
	
	
	private void send(String msg) {
		
		try {
			connection=connectionFactory.createConnection();
	        connection.start();
            // Create a Session
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create the destination (Topic or Queue)
            Destination destination = session.createQueue(queue);
            // Create a MessageProducer from the Session to the Topic or Queue
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            TextMessage message = session.createTextMessage(msg);

            // Tell the producer to send the message
            System.out.println("Sent message: "+ message.hashCode() + " : " + Thread.currentThread().getName());
            producer.send(message);
            // Clean up
            session.close();
            connection.close();	        
            
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	} //
	
	
	private void consume(String queue) {
		
		try {
			connection=connectionFactory.createConnection();
	       // connection.start();
            // Create a Session
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create the destination (Topic or Queue)
            Destination destination = session.createQueue(queue);

            // Create a MessageConsumer from the Session to the Topic or Queue
            MessageConsumer consumer = session.createConsumer(destination);
            connection.start();
            // Wait for a message
            String m="";
            while  (!m.equals("quit")) {
                Message message = consumer.receive(2000);
	            if (message instanceof TextMessage) {
	                TextMessage textMessage = (TextMessage) message;
	                String text = textMessage.getText();
	                m=text;
	                System.out.println("Received: " + text);
	            } else {
	                System.out.println("Received: " + message);
	            }
            }
            consumer.close();
            session.close();
            connection.close();      
            
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	
}
