package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.common.requests.FetchResponse;

public class FetchResponseBackgroundEvent extends BackgroundEvent {
    private final FetchResponse fetchResponse;
    public FetchResponseBackgroundEvent(FetchResponse response) {
        super();
        this.fetchResponse = response;
    }
}
