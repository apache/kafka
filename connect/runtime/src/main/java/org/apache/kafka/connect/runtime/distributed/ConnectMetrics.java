package org.apache.kafka.connect.runtime.distributed;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.SpecificMetrics;
import org.apache.kafka.common.network.SelectorMetricsRegistry;

public class ConnectMetrics {

    public WorkerCoordinatorMetricsRegistry coordinatorMetrics;
    public SelectorMetricsRegistry selectorMetrics;

    public ConnectMetrics(Set<String> tags, String metricGrpPrefix) {
        this.coordinatorMetrics = new WorkerCoordinatorMetricsRegistry(tags, metricGrpPrefix);
        this.selectorMetrics = new SelectorMetricsRegistry(tags, metricGrpPrefix);

    }
    
    private List<MetricNameTemplate> getAllTemplates() {
        List<MetricNameTemplate> l = new ArrayList<>();
        l.addAll(this.coordinatorMetrics.getAllTemplates());
        return l;
    }


    public static void main(String[] args) {
        Set<String> tags = new HashSet<>();
        tags.add("client-id");
        ConnectMetrics metrics = new ConnectMetrics(tags, "connect");
        System.out.println(SpecificMetrics.toHtmlTable("kafka.connect", metrics.getAllTemplates()));

    }

}
