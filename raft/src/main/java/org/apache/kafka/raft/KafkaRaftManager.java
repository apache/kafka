package org.apache.kafka.raft;

public class KafkaRaftManager<T> {

    private static class Builder<T> {
        private KafkaNetworkChannel network;
        private KafkaRaftClient<T> client;
        private ReplicatedLog log;


    }
}
