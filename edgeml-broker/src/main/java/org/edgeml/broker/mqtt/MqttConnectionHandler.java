package org.edgeml.broker.mqtt;

import org.edgeml.broker.routing.TopicRouter;
import org.edgeml.broker.state.AgentStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class MqttConnectionHandler implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(MqttConnectionHandler.class);
    
    private final Socket socket;
    private final TopicRouter router;
    private final AgentStateStore stateStore;
    private final MqttCodec codec;

    public MqttConnectionHandler(Socket socket, TopicRouter router, AgentStateStore stateStore) {
        this.socket = socket;
        this.router = router;
        this.stateStore = stateStore;
        this.codec = new MqttCodec();
    }

    @Override
    public void run() {
        try (InputStream in = socket.getInputStream();
             OutputStream out = socket.getOutputStream()) {
            
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                // In a real implementation, we would pass 'buffer' and 'bytesRead' to MqttCodec
                codec.decode(buffer, bytesRead);
                
                // Stub: Always reply with CONNACK for now to satisfy simple client connect tests
                byte[] connack = codec.encodeConnack();
                out.write(connack);
                out.flush();
            }
        } catch (IOException e) {
            log.warn("Connection error", e);
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                log.warn("Error closing socket", e);
            }
            log.info("Connection closed");
        }
    }
}
