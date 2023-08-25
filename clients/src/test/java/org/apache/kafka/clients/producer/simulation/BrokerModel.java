package org.apache.kafka.clients.producer.simulation;

import org.apache.kafka.common.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

class BrokerModel {
    final Node node;
    final ClusterModel cluster;
    final List<ConnectionModel> active;
    final List<ConnectionModel> disconnecting;

    BrokerModel(
        Node node,
        ClusterModel cluster
    ) {
        this.node = node;
        this.cluster = cluster;
        this.active = new ArrayList<>();
        this.disconnecting = new ArrayList<>();
    }

    void connect(ConnectionModel connection) {
        this.active.add(connection);
    }

    void disconnect(ConnectionModel connection) {
        // We want to model delivery of some or all of the requests sent
        // by the client after a disconnect. Instead of immediately removing
        // the connection from the active pool, we drop one or more of the
        // undelivered requests. Once these requests have been processed,
        // then the connection will be removed.
        connection.maybeDropUndeliveredRequests();
        this.disconnecting.add(connection);
    }

    ConnectionModel randomConnection(Random random) {
        if (active.isEmpty()) {
            return null;
        } else {
            int randomIdx = random.nextInt(active.size());
            ConnectionModel connection = active.get(randomIdx);

            if (disconnecting.contains(connection) && !connection.hasUndeliveredRequests()) {
                active.remove(connection);
                disconnecting.remove(connection);
            }

            return connection;
        }
    }
}
