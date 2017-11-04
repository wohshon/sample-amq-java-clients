package com.demo.jms.client;

import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class JMSClient {
	
//java -cp /home/virtuser/workspace10/activemq.client/amqclient.jar com.demo.jms.client.JMSClient http-remoting://192.168.223.130:8080 jmsuser jboss.1234 jms/topic/DemoTopic t 'hello world' jms/RemoteConnectionFactory 1 
//java -cp /home/virtuser/workspace10/activemq.client/amqclient.jar com.demo.jms.client.JMSClient http-remoting://192.168.223.130:8080 jmsuser jboss.1234 jms/topic/DemoTopic t 'dummy' jms/RemoteConnectionFactory 0 


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
            namingContext = new InitialContext(env);
            connectionFactory = (ConnectionFactory) namingContext
                    .lookup(jndilookup);
            System.out.println("Got ConnectionFactory " + jndilookup);    		
            
            
    	} catch (Exception e) {
			e.printStackTrace();
		}   	
    }	//constructor
    
    private void sendMessage(String destName, String destType, String msg) {
    	
    	
    	try {
			Object dest=this.namingContext.lookup(destName);
            connection=connectionFactory.createConnection(
            		env.getProperty(Context.SECURITY_PRINCIPAL),
            		env.getProperty(Context.SECURITY_CREDENTIALS));
            
            session=connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            TextMessage message=session.createTextMessage(msg);
            MessageProducer messageProducer = null;
            if (destType.equals("q")) {
            	messageProducer=session.createProducer((Queue)dest);
            } else {
            	messageProducer=session.createProducer((Topic)dest);
            }
            messageProducer.send(message);
            connection.start();
            System.out.println("Message "+msg+" sent to "+dest);
            
		} catch (NamingException | JMSException e) {
			e.printStackTrace();
		}  finally {
			try {
				if (connection!=null) 	connection.close();
				if (namingContext!=null) 	namingContext.close();
			} catch (JMSException e) {
				e.printStackTrace();
			} catch (NamingException e) {
				e.printStackTrace();
			}
		}//finally    	
    	
    }//
    
    private void consumeMessage(String destName, String destType) {
    	
    	
    	try {
			Object dest=this.namingContext.lookup(destName);
            connection=connectionFactory.createConnection(
            		env.getProperty(Context.SECURITY_PRINCIPAL),
            		env.getProperty(Context.SECURITY_CREDENTIALS));
            
            session=connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer messageConsumer = null;
            if (destType.equals("q")) {
            	messageConsumer=session.createConsumer((Queue)dest);
            } else {
            	messageConsumer=session.createConsumer((Topic)dest);
            }
            
            connection.start();
            String m="";
            while  (!m.equals("quit")) {
            TextMessage text = (TextMessage)messageConsumer.receive();
            System.out.println("Message "+text.getText()+" recv from "+dest);
            m=text.getText();
            }
            
		} catch (NamingException | JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				if (connection!=null) 	connection.close();
				if (namingContext!=null) 	namingContext.close();
			} catch (JMSException e) {
				e.printStackTrace();
			} catch (NamingException e) {
				e.printStackTrace();
			}
		}//finally 
    	
    }
    
}
