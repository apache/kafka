package org.apache.kafka.streams.state;

import org.apache.kafka.common.header.Headers;

public interface AggregationWithHeaders<AGG> {
    AGG aggregate();
    Headers headers();

    // maybe need other timestamp methods
}
