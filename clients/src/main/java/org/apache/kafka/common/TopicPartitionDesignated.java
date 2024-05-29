package org.apache.kafka.common;

public class TopicPartitionDesignated extends TopicPartition{

    private int designatedLeader = -1;

    public TopicPartitionDesignated(String topic, int partition) {
        super(topic, partition);
    }

    public void setDesignatedLeader(int designatedLeader) {
        this.designatedLeader = designatedLeader;
    }

    public int getDesignatedLeader(){ return designatedLeader; }

}
