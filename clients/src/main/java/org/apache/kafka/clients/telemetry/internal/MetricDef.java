package org.apache.kafka.clients.telemetry.internal;

import java.util.Objects;
import java.util.Set;

public class MetricDef {

    private final String name;

    private final MetricType metricType;

    private final Set<String> labels;

    public MetricDef(String name, MetricType metricType, Set<String> labels) {
        this.name = name;
        this.metricType = metricType;
        this.labels = labels;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final MetricDef metricDef = (MetricDef) o;
        return name.equals(metricDef.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

}
