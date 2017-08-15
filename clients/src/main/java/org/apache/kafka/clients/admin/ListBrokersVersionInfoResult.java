package org.apache.kafka.clients.admin;

import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.internals.KafkaFutureImpl;

import java.util.Map;

/**
 * The result of the {@link KafkaAdminClient#listBrokersVersionInfo()} call.
 *
 * The API of this class is evolving, see {@link AdminClient} for details.
 */
@InterfaceStability.Evolving
public class ListBrokersVersionInfoResult {

    private final Map<Node, KafkaFutureImpl<NodeApiVersions>> brokerVersionsInfoFutures;

    public ListBrokersVersionInfoResult(Map<Node, KafkaFutureImpl<NodeApiVersions>> brokerVersionsInfoFutures) {
        this.brokerVersionsInfoFutures = brokerVersionsInfoFutures;
    }

    public Map<Node, KafkaFutureImpl<NodeApiVersions>> getBrokerVersionsInfoFutures() {
        return brokerVersionsInfoFutures;
    }
}
