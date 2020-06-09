package org.apache.kafka.trogdor.coordinator;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;

import java.util.ArrayList;
import java.util.List;

public class TrogdorMetrics implements AutoCloseable {
    private final Sensor createdTasks;
    private final Sensor runningTasks;
    private final Sensor doneTasks;
    private final Sensor activeAgents;

    private final Metrics metrics;
    private final List<Sensor> sensors = new ArrayList<>();

    public TrogdorMetrics(Metrics metrics, String metricGrpPrefix) {
        this.metrics = metrics;
        String metricGroupName = metricGrpPrefix + "-metrics";

        this.createdTasks = sensor("tasks-created");
        MetricName createdTasksMetricName = metrics.metricName("created-task-count",
                metricGroupName, "The total number of created tasks in the Trogdor cluster");
        this.createdTasks.add(createdTasksMetricName, new CumulativeSum());

        this.runningTasks = sensor("tasks-running");
        MetricName runningTasksMetricName = metrics.metricName("running-task-count",
                metricGroupName, "The total number of running tasks in the Trogdor cluster");
        this.runningTasks.add(runningTasksMetricName, new CumulativeSum());

        this.doneTasks = sensor("tasks-done");
        MetricName doneTasksMetricName = metrics.metricName("done-task-count",
                metricGroupName, "The total number of done tasks in the Trogdor cluster");
        this.doneTasks.add(doneTasksMetricName, new CumulativeSum());

        this.activeAgents = sensor("active-agents");
        MetricName activeAgentsMetricName = metrics.metricName("active-agents-count",
                metricGroupName, "The total number of active agents in the Trogdor cluster");
        this.activeAgents.add(activeAgentsMetricName, new CumulativeSum());
    }

    private Sensor sensor(String name, Sensor... parents) {
        Sensor sensor = metrics.sensor(name, parents);
        sensors.add(sensor);
        return sensor;
    }

    @Override
    public void close() {
        for (Sensor sensor : sensors)
            metrics.removeSensor(sensor.name());
        metrics.close();
    }

    public Metrics getMetrics() {
        return metrics;
    }

    public void recordCreatedTask() {
        this.createdTasks.record(1);
    }

    public void recordRunningTask() {
        this.runningTasks.record(1);
    }

    public void recordDoneTask() {
        this.doneTasks.record(1);
    }

    public void recordActiveAgent() {
        this.activeAgents.record(1);
    }
}