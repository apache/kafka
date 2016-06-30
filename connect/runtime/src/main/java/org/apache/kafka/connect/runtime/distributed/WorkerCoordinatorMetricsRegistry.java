package org.apache.kafka.connect.runtime.distributed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.clients.consumer.internals.AbstractCoordinatorMetrics;
import org.apache.kafka.common.MetricNameTemplate;

public class WorkerCoordinatorMetricsRegistry
        extends AbstractCoordinatorMetrics {

    public MetricNameTemplate assignedConnectors;
    public MetricNameTemplate assignedTasks;

    public WorkerCoordinatorMetricsRegistry(String metricGrpPrefix) {
        this(new HashSet<String>(), metricGrpPrefix);
    }

    public WorkerCoordinatorMetricsRegistry(Set<String> metricsTags,
            String metricGrpPrefix) {
        super(metricsTags, metricGrpPrefix);

        String groupName = metricGrpPrefix + "-coordinator-metrics";
        this.assignedConnectors = new MetricNameTemplate("assigned-connectors", groupName,
                "The number of connector instances currently assigned to this consumer", metricsTags);
        this.assignedTasks = new MetricNameTemplate("assigned-tasks", groupName,
                "The number of tasks currently assigned to this consumer", metricsTags);
    }
    
    @Override
    public List<MetricNameTemplate> getAllTemplates() {
        List<MetricNameTemplate> l = new ArrayList<>(super.getAllTemplates());
        l.addAll(Arrays.asList(
                assignedConnectors,
                assignedTasks
            ));
        return l;
    }


}
