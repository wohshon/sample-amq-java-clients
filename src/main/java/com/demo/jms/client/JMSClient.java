package com.demo.jms.client;

import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.Context;
import javax.naming.InitialContext;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSslConnectionFactory;

public class JMSClient {
	
//java -cp /home/virtuser/workspace10/activemq.client/amqclient.jar com.demo.jms.client.JMSClient http-remoting://192.168.223.130:8080 jmsuser jboss.1234 jms/topic/DemoTopic t 'hello world' jms/RemoteConnectionFactory 1 
//java -cp /home/virtuser/workspace10/activemq.client/amqclient.jar com.demo.jms.client.JMSClient http-remoting://192.168.223.130:8080 jmsuser jboss.1234 jms/topic/DemoTopic t 'dummy' jms/RemoteConnectionFactory 0 
	
//for non jndi cases, jndilookup ="amq" , will use AMQConnection factory	


	public static void main(String[] args) {
	    if( args==null || args.length == 0 || args.length !=8)
	    {
	        System.out.println("Proper Usage is: java -cp amqclient.jar com.demo.jms.client.JMSClient <broker url> <username> <password> <destination name> <dest-type> <content> <jndi> <mode> ");
	        System.exit(0);
	    } else {
	        String broker       = args[0];
	        String username= args[1];
	        String password= args[2];
			String dest        = args[3];
			String destType        = args[4];
	        String content = args[5];
	        String jndilookup=args[6];
			String mode        = args[7];
	        
			JMSClient client=new JMSClient(broker,username,password,jndilookup);
	        if (mode.equals("0")) {
	        	//consume
	        	System.out.println("consuming....");
	        	client.consumeMessage(dest, destType);
	        	
	        } else {
	        	System.out.println("sending....");

	        	client.sendMessage(dest, destType, content);
	        }
			
	    }// if arg
	}//main

    Context namingContext = null;
    Connection connection=null;
    ConnectionFactory connectionFactory=null;
    Session session=null;
    Properties env=null;
    
    public JMSClient(String url, String username, String password, String jndilookup) {
    	try {
    		
    		env = new Properties();
    		env.put(Context.INITIAL_CONTEXT_FACTORY,
                    "org.jboss.naming.remote.client.InitialContextFactory");
            env.put(Context.PROVIDER_URL, url);
            env.put(Context.SECURITY_PRINCIPAL, username);
            env.put(Context.SECURITY_CREDENTIALS, password);

            if (jndilookup.equals("amq")) {
            	connectionFactory = (ActiveMQConnectionFactory)new ActiveMQSslConnectionFactory(url);
            	((ActiveMQSslConnectionFactory)connectionFactory).setUserName(username);
            	((ActiveMQSslConnectionFactory)connectionFactory).setPassword(password);
				((ActiveMQSslConnectionFactory)connectionFactory).setTrustStore("file:/home/virtuser/keystores/demo/client.ts");
				((ActiveMQSslConnectionFactory)connectionFactory).setTrustStorePassword("password");
				((ActiveMQSslConnectionFactory)connectionFactory).setBrokerURL("failover://"+url);;
            }
            else {
                namingContext = new InitialContext(env);
                connectionFactory = (ConnectionFactory) namingContext .lookup(jndilookup);
                System.out.println("Got ConnectionFactory " + jndilookup);    		
            }
            
    	} catch (Exception e) {
			e.printStackTrace();
		}   	
    }	//constructor
    
    private void sendMessage(String destName, String destType, String msg) {
    	
    	
    	try {
    		Destination dest=null;
            String m=null;    		
    		//dest=(Destination)this.namingContext.lookup(destName);


            connection=connectionFactory.createConnection(
            		env.getProperty(Context.SECURITY_PRINCIPAL),
            		env.getProperty(Context.SECURITY_CREDENTIALS));
            
            session=connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            TextMessage message=session.createTextMessage(msg);
            MessageProducer messageProducer = null;
            if (destType.equals("q")) {
                dest = (Queue)session.createQueue(destName);
            	messageProducer=session.createProducer(dest);
            } else {
                dest = (Topic)session.createTopic(destName);
            	messageProducer=session.createProducer(dest);
            }
            messageProducer.send(message);
            connection.start();
            int i=0;
            while (true) {
            	m=msg+"_"+i++;
            	message.setText(m);
            	messageProducer.send(message);
            	System.out.println("Message "+m+" sent to "+dest);
            	try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					System.out.println("awoke..." );
				}
            	
            }            
		} catch (JMSException e) {
			e.printStackTrace();
		}  finally {
			try {
				if (connection!=null) 	connection.close();
			} catch (JMSException e) {
				e.printStackTrace();
			} 
		}//finally    	
    	
    }//
    
    private void consumeMessage(String destName, String destType) {
    	
    	
    	try {
    		Destination dest=null;
            String m="";
        
    		//dest=(Destination)this.namingContext.lookup(destName);
            connection=connectionFactory.createConnection(
            		env.getProperty(Context.SECURITY_PRINCIPAL),
            		env.getProperty(Context.SECURITY_CREDENTIALS));
            
            session=connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer messageConsumer = null;
            if (destType.equals("q")) {
                dest = (Queue)session.createQueue(destName);
            	messageConsumer=session.createConsumer(dest);
            } else {
                dest = (Topic)session.createTopic(destName);
            	messageConsumer=session.createConsumer(dest);
            }
            connection.start();
            while  (!m.equals("quit")) {    

            TextMessage text = (TextMessage)messageConsumer.receive();
            System.out.println("Message "+text.getText()+" recv from "+dest);
            m=text.getText();
            }
            
		} catch ( JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				if (connection!=null) 	connection.close();
			} catch (JMSException e) {
				e.printStackTrace();
			} 
		}//finally 
    	
    }
    
}
