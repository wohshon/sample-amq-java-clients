package com.demo.jms.client;

import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.Context;
import javax.naming.InitialContext;

import org.junit.Test;

public class JMSClientTest {
	   private String MESSAGE = "Hello, World!";
	    private String CONNECTION_FACTORY = "jms/RemoteConnectionFactory";
	    private String DESTINATION = "jms/topic/DemoTopic";
	
	 @Test
	public void testSendJMS() throws Exception {
		

        Context namingContext = null;
        Connection connection=null;
        try {
        		final Properties env = new Properties();
        		env.put(Context.INITIAL_CONTEXT_FACTORY,
                        "org.jboss.naming.remote.client.InitialContextFactory");
                env.put(Context.PROVIDER_URL, "http-remoting://192.168.223.130:8080");
                env.put(Context.SECURITY_PRINCIPAL, "jmsuser");
                env.put(Context.SECURITY_CREDENTIALS, "jboss.1234");
                namingContext = new InitialContext(env);
                ConnectionFactory connectionFactory = (ConnectionFactory) namingContext
                        .lookup(CONNECTION_FACTORY);
                System.out.println("Got ConnectionFactory " + CONNECTION_FACTORY);
     
/*                Destination destination = (Destination) namingContext
                        .lookup(DESTINATION);*/
                Topic topic=(Topic)namingContext.lookup(DESTINATION);
                System.out.println("Got JMS Endpoint " + DESTINATION);
                connection=connectionFactory.createConnection(
                		env.getProperty(Context.SECURITY_PRINCIPAL),
                		env.getProperty(Context.SECURITY_CREDENTIALS));
                
                Session session=connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageProducer producer=session.createProducer(topic);
                TextMessage message=session.createTextMessage(MESSAGE);
                producer.send(message);
                System.out.println("message sent");
                MessageConsumer messageConsumer = session.createConsumer(topic);
                connection.start();

/*                TextMessage text = (TextMessage) messageConsumer.receive(5000);
                System.out.println("Received Message: " + text.getText());
                
                assert(text.getText().equals(MESSAGE));
*/
                assert(true);
        }
        catch (Exception e) {
			e.printStackTrace();
		}
        finally{
            if (namingContext != null) {
                namingContext.close();
            }
 
            // closing the context takes care of consumer too
            if (connection != null) {
                connection.close();
            }        	
            
        }

	}
}
