package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SampleSinkTask extends SinkTask {

    private Set<TopicPartition> currentAssignment;
    @Override
    public String version() {
        return null;
    }

    public void open(Collection<TopicPartition> topicPartitions) {
        this.currentAssignment = context.assignment();
    }

    public void close(Collection<TopicPartition> topicPartitions) {
        this.currentAssignment = context.assignment();
    }

    @Override
    public void start(Map<String, String> props) {
        this.currentAssignment = new HashSet<>();
    }

    @Override
    public void put(Collection<SinkRecord> records) {

    }

    public Set<TopicPartition> getCurrentAssignment() {
        return this.currentAssignment;
    }

    @Override
    public void stop() {

    }
}
