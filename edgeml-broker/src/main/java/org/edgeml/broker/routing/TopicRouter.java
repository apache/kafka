package org.edgeml.broker.routing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;

public class TopicRouter {
    private static final Logger log = LoggerFactory.getLogger(TopicRouter.class);
    private final ConcurrentHashMap<String, Set<String>> subscriptions;

    public TopicRouter() {
        this.subscriptions = new ConcurrentHashMap<>();
    }

    public void subscribe(String topic, String clientId) {
        subscriptions.computeIfAbsent(topic, k -> ConcurrentHashMap.newKeySet()).add(clientId);
        log.info("Client {} subscribed to topic {}", clientId, topic);
    }

    public void route(String topic, byte[] payload) {
        log.info("Routing message to topic {}", topic);
        // Placeholder: Will implement semantic Vector API routing here in future PRs
    }
}
