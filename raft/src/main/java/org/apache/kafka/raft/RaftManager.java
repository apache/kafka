package org.apache.kafka.raft;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.server.common.serialization.RecordSerde;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface RaftManager<T> {

    CompletableFuture<ApiMessage> handleRequest(
        RequestContext context,
        RequestHeader header,
        ApiMessage request,
        long createdTimeMs
    );

    void register(RaftClient.Listener<T> listener);

    LeaderAndEpoch leaderAndEpoch();

    RaftClient<T> client();

    ReplicatedLog replicatedLog();

    Optional<Node> voterNode(int id, ListenerName listener);

    RecordSerde<T> recordSerde();
}
