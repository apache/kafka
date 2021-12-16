package org.apache.kafka.clients.telemetry.internal;

import java.io.Closeable;
import java.time.Duration;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

public class TelemetryClient implements Closeable {

    private final TelemetrySender sender;

    private String clientInstanceId;

    public TelemetryClient(KafkaClient client, Metadata metadata, Time time, LogContext logContext) {
        this.sender = new TelemetrySender(client, metadata, time, logContext);

        // TODO: start a thread, etc...
    }

    public TelemetryClient(KafkaClient client, Metadata metadata, LogContext logContext) {
        this(client, metadata, Time.SYSTEM, logContext);
    }

    public static boolean isTelemetryEnabled() {
        // TODO: look at config and determine setting, maybe log...
        return true;
    }

    public String clientInstanceId(Duration timeout) {
        // TODO: R/W locking
        if (clientInstanceId != null) {
            clientInstanceId = sender.telemetrySubscription(timeout).getClientInstanceId().toString();
        }

        return clientInstanceId;
    }

    @Override
    public void close() {
        // TOOD: shut down sender, thread, etc...
    }

}
