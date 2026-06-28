package org.edgeml.broker;

import org.edgeml.broker.mqtt.MqttConnectionHandler;
import org.edgeml.broker.routing.TopicRouter;
import org.edgeml.broker.state.AgentStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class EdgeBroker {
    private static final Logger log = LoggerFactory.getLogger(EdgeBroker.class);
    private static final int PORT = 1883;

    private final TopicRouter router;
    private final AgentStateStore stateStore;
    private final ExecutorService virtualThreadExecutor;

    public EdgeBroker() {
        this.router = new TopicRouter();
        this.stateStore = new AgentStateStore();
        this.virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public void start() {
        log.info("Starting EdgeML-Broker on port {}", PORT);
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            while (!Thread.currentThread().isInterrupted()) {
                Socket clientSocket = serverSocket.accept();
                log.info("Accepted connection from {}", clientSocket.getRemoteSocketAddress());
                virtualThreadExecutor.submit(new MqttConnectionHandler(clientSocket, router, stateStore));
            }
        } catch (IOException e) {
            log.error("Broker server socket error", e);
        }
    }

    public static void main(String[] args) {
        EdgeBroker broker = new EdgeBroker();
        broker.start();
    }
}
