package org.edgeml.broker;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class EdgeBrokerTest {
    
    private static Thread brokerThread;
    
    @BeforeAll
    static void startBroker() throws InterruptedException {
        brokerThread = new Thread(() -> {
            EdgeBroker broker = new EdgeBroker();
            broker.start();
        });
        brokerThread.start();
        // Give broker time to start
        Thread.sleep(1000);
    }
    
    @AfterAll
    static void stopBroker() {
        if (brokerThread != null) {
            brokerThread.interrupt();
        }
    }

    @Test
    void testBasicConnection() {
        String broker = "tcp://localhost:1883";
        String clientId = "test-client";
        MemoryPersistence persistence = new MemoryPersistence();

        try {
            MqttClient sampleClient = new MqttClient(broker, clientId, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            System.out.println("Connecting to broker: " + broker);
            sampleClient.connect(connOpts);
            System.out.println("Connected");
            
            assertTrue(sampleClient.isConnected());
            
            sampleClient.disconnect();
            System.out.println("Disconnected");
        } catch (MqttException me) {
            me.printStackTrace();
            org.junit.jupiter.api.Assertions.fail("Connection failed: " + me.getMessage());
        }
    }
}
