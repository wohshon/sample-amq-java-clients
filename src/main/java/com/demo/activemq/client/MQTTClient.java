package com.demo.activemq.client;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class MQTTClient {

	public static void main(String[] args) {
	
	    if( args==null || args.length == 0)
	    {
	        System.out.println("Proper Usage is: java -cp amqclient.jar com.demo.activemq.client.MQTTClient <broker url: tcp://host:port> <username> <password> <client id> <topic name> <content>");
	        System.exit(0);
	    }		
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
            //connOpts.set
            connOpts.setCleanSession(true);
            System.out.println("Connecting to broker: "+broker);
            sampleClient.connect(connOpts);
            System.out.println("Connected");
            if (sub!=null && !sub.equals("1")) {
            	System.out.println("Publishing message: "+content);
            	MqttMessage message = new MqttMessage(content.getBytes());
            	message.setQos(qos);
            	sampleClient.publish(topic, message);
            	System.out.println("Message published");
                sampleClient.disconnect();
                System.out.println("Disconnected");            	
                System.exit(0);
            }//
            else {
            	System.out.println("Subscribed to topic "+topic);
            	sampleClient.subscribe(topic,0);
            	sampleClient.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable cause) {

                }

                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                  System.out.println("Got message from "+topic+ ":"+message.toString());
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {

                }
             });   
            
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
}
