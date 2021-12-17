package org.apache.kafka.clients.telemetry.internal;

public enum MetricLabel {

    brokerId("broker_id"), reason(), requestType("request_type");

    private final String labelName;

    MetricLabel(String labelName) {
        this.labelName = labelName;
    }

    MetricLabel() {
        this(null);
    }

    public String labelName() {
        if (labelName != null)
            return labelName;
        else
            return name();
    }

}
