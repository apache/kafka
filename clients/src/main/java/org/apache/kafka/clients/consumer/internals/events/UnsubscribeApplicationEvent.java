package org.apache.kafka.clients.consumer.internals.events;

public class UnsubscribeApplicationEvent extends ApplicationEvent {

    public UnsubscribeApplicationEvent() {
        super(Type.UNSUBSCRIBE);
    }
}
