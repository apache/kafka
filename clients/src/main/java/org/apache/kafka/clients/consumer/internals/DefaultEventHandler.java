package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.ConsumerRequestEvent;
import org.apache.kafka.clients.consumer.internals.events.ConsumerResponseEvent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class DefaultEventHandler implements EventHandler<ConsumerRequestEvent, ConsumerResponseEvent> {
    BlockingQueue<ConsumerRequestEvent> consumerRequestEvents;
    BlockingQueue<ConsumerResponseEvent> consumerResponseEvents;

    public DefaultEventHandler(ConsumerConfig config) {
        this.consumerRequestEvents = new LinkedBlockingQueue<>();
        this.consumerResponseEvents = new LinkedBlockingQueue<>();
    }

    @Override
    public ConsumerResponseEvent poll() {
        return consumerResponseEvents.poll();
    }

    @Override
    public boolean add(ConsumerRequestEvent event) {
        return consumerRequestEvents.add(event);
    }
}
