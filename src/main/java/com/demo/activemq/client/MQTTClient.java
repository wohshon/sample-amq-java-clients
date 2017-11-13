package com.demo.activemq.client;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class MQTTClient {

	public static void main(String[] args) {
	
	    if( args==null || args.length == 0)
	    {
	        System.out.println("Proper Usage is: java -cp amqclient.jar com.demo.activemq.client.MQTTClient <broker url: tcp://host:port> <username> <password> <client id> <topic name> <content>");
	        System.exit(0);
	    }		
// java -cp /home/virtuser/workspace10/activemq.client/amqclient.jar com.demo.activemq.client.MQTTClient tcp://192.168.223.130:10883 admin admin client123 SENSOR/PSI/1/DATA  "dummy" 1
	    
/*        String topic        = "order.mqtt.topic";
        //String content      = "Message from MqttPublishSample";
        String content = "<order><orderId>1</orderId><orderItems>	<orderItemId>1</orderItemId>	<orderItemQty>1</orderItemQty><orderItemPublisherName>Orly</orderItemPublisherName><orderItemPrice>10.59</orderItemPrice></orderItems></order>";
        int qos             = 2;
        String broker       = "tcp://54.179.188.119:30001";
        String clientId     = "JavaSample";
*/
        String broker       = args[0];
        String username= args[1];
        String password= args[2];
        String clientId     = args[3];
		String topic        = args[4];
		String sub        = args[6];
        //String content      = "Message from MqttPublishSample";
        String content = args[5];
        int qos             = 2;
        
        
        MemoryPersistence persistence = new MemoryPersistence();

        try {
        	
            MqttClient sampleClient = new MqttClient(broker, clientId, persistence);
            
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setUserName(username);
            connOpts.setPassword(password.toCharArray());
           //use the connectionlost call back to introduce a delay
            //use custom logic, so did not turn this on
            // connOpts.setAutomaticReconnect(true);
            //connOpts.set
//            connOpts.setCleanSession(true);
            connOpts.setCleanSession(false); // to test reconnectoin

            MqttCallback callback=new MqttCallback() {
				
				@Override
				public void messageArrived(String arg0, MqttMessage msg) throws Exception {
		            System.out.println("message arrvied "+msg.toString());
					
				}
				
				@Override
				public void deliveryComplete(IMqttDeliveryToken token) {
			            System.out.println("Delivery done "+token);

					
				}
				
				@Override
				public void connectionLost(Throwable arg0) {
					System.out.println("Lost connection");
					
					try {
						System.out.println("Reconnecting....in 1 sec");
						Thread.sleep(1000);
						if (!sampleClient.isConnected()) {
							sampleClient.connect(connOpts);
						}
						if (sub.equals("0")) {
							sampleClient.subscribe(topic,0);
						} else {
				            send(sampleClient, topic, qos, content, sub);
						}
						System.out.println("done");

					} catch (MqttException | InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			};
            System.out.println("setting callback: "+broker);
            System.out.println("Connecting to broker: "+broker);

            sampleClient.setCallback(callback);
            sampleClient.connect(connOpts);
            System.out.println("Connected");
            if (sub!=null && !sub.equals("0")) {
               send(sampleClient, topic, qos, content, sub);
                sampleClient.disconnect();
                System.out.println("Disconnected");            	
                System.exit(0);
            }//
            else { // sub =1 
            	System.out.println("Subscribed to topic "+topic);
            	sampleClient.subscribe(topic,0);            
            }//
            
        } catch(MqttException me) {
            System.out.println("reason "+me.getReasonCode());
            System.out.println("msg "+me.getMessage());
            System.out.println("loc "+me.getLocalizedMessage());
            System.out.println("cause "+me.getCause());
            System.out.println("excep "+me);
            me.printStackTrace();
        }		
	}
	
	private static void send(MqttClient sampleClient, String topic, int qos, String content, String sub) {
        	
        	while (!content.equals("quit")) {
            	System.out.println("Publishing message: "+content);
            	
            	MqttMessage message = new MqttMessage(content.getBytes());
            	message.setQos(qos);
            	try {
					sampleClient.publish(topic, message);
				} catch (MqttException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
            	System.out.println("Message published");
            	try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
        	}		
	}
}
