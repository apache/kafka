/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.server.config;

import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.server.common.MetadataVersion;

public class ReplicationConfigs {
    public static final String CONTROLLER_SOCKET_TIMEOUT_MS_CONFIG = "controller.socket.timeout.ms";
    public static final int CONTROLLER_SOCKET_TIMEOUT_MS_DEFAULT = 30000;
    public static final String CONTROLLER_SOCKET_TIMEOUT_MS_DOC = "The socket timeout for controller-to-broker channels.";

    public static final String DEFAULT_REPLICATION_FACTOR_CONFIG = "default.replication.factor";
    public static final int REPLICATION_FACTOR_DEFAULT = 1;
    public static final String DEFAULT_REPLICATION_FACTOR_DOC = "The default replication factors for automatically created topics.";

    public static final String REPLICA_LAG_TIME_MAX_MS_CONFIG = "replica.lag.time.max.ms";
    public static final long REPLICA_LAG_TIME_MAX_MS_DEFAULT = 30000L;
    public static final String REPLICA_LAG_TIME_MAX_MS_DOC = "If a follower hasn't sent any fetch requests or hasn't consumed up to the leaders log end offset for at least this time," +
            " the leader will remove the follower from isr";

    public static final String REPLICA_SOCKET_TIMEOUT_MS_CONFIG = "replica.socket.timeout.ms";
    public static final int REPLICA_SOCKET_TIMEOUT_MS_DEFAULT = 30 * 1000;
    public static final String REPLICA_SOCKET_TIMEOUT_MS_DOC = "The socket timeout for network requests. Its value should be at least replica.fetch.wait.max.ms";

    public static final String REPLICA_SOCKET_RECEIVE_BUFFER_BYTES_CONFIG = "replica.socket.receive.buffer.bytes";
    public static final int REPLICA_SOCKET_RECEIVE_BUFFER_BYTES_DEFAULT = 64 * 1024;
    public static final String REPLICA_SOCKET_RECEIVE_BUFFER_BYTES_DOC = "The socket receive buffer for network requests to the leader for replicating data";

    public static final String REPLICA_FETCH_MAX_BYTES_CONFIG = "replica.fetch.max.bytes";
    public static final int REPLICA_FETCH_MAX_BYTES_DEFAULT = 1024 * 1024;
    public static final String REPLICA_FETCH_MAX_BYTES_DOC = "The number of bytes of messages to attempt to fetch for each partition. This is not an absolute maximum, " +
            "if the first record batch in the first non-empty partition of the fetch is larger than this value, the record batch will still be returned " +
            "to ensure that progress can be made. The maximum record batch size accepted by the broker is defined via " +
            "<code>message.max.bytes</code> (broker config) or <code>max.message.bytes</code> (topic config).";

    public static final String REPLICA_FETCH_WAIT_MAX_MS_CONFIG = "replica.fetch.wait.max.ms";
    public static final int REPLICA_FETCH_WAIT_MAX_MS_DEFAULT = 500;
    public static final String REPLICA_FETCH_WAIT_MAX_MS_DOC = "The maximum wait time for each fetcher request issued by follower replicas. This value should always be less than the " +
            "replica.lag.time.max.ms at all times to prevent frequent shrinking of ISR for low throughput topics";

    public static final String REPLICA_FETCH_MIN_BYTES_CONFIG = "replica.fetch.min.bytes";
    public static final int REPLICA_FETCH_MIN_BYTES_DEFAULT = 1;
    public static final String REPLICA_FETCH_MIN_BYTES_DOC = "Minimum bytes expected for each fetch response. If not enough bytes, wait up to <code>replica.fetch.wait.max.ms</code> (broker config).";

    public static final String REPLICA_FETCH_BACKOFF_MS_CONFIG = "replica.fetch.backoff.ms";
    public static final int REPLICA_FETCH_BACKOFF_MS_DEFAULT = 1000;
    public static final String REPLICA_FETCH_BACKOFF_MS_DOC = "The amount of time to sleep when fetch partition error occurs.";

    public static final String REPLICA_FETCH_RESPONSE_MAX_BYTES_CONFIG = "replica.fetch.response.max.bytes";
    public static final int REPLICA_FETCH_RESPONSE_MAX_BYTES_DEFAULT = 10 * 1024 * 1024;
    public static final String REPLICA_FETCH_RESPONSE_MAX_BYTES_DOC = "Maximum bytes expected for the entire fetch response. Records are fetched in batches, " +
            "and if the first record batch in the first non-empty partition of the fetch is larger than this value, the record batch " +
            "will still be returned to ensure that progress can be made. As such, this is not an absolute maximum. The maximum " +
            "record batch size accepted by the broker is defined via <code>message.max.bytes</code> (broker config) or " +
            "<code>max.message.bytes</code> (topic config).";

    public static final String NUM_REPLICA_FETCHERS_CONFIG = "num.replica.fetchers";
    public static final int NUM_REPLICA_FETCHERS_DEFAULT = 1;
    public static final String NUM_REPLICA_FETCHERS_DOC = "Number of fetcher threads used to replicate records from each source broker. The total number of fetchers " +
            "on each broker is bound by <code>num.replica.fetchers</code> multiplied by the number of brokers in the cluster." +
            "Increasing this value can increase the degree of I/O parallelism in the follower and leader broker at the cost " +
            "of higher CPU and memory utilization.";

    public static final String REPLICA_HIGH_WATERMARK_CHECKPOINT_INTERVAL_MS_CONFIG = "replica.high.watermark.checkpoint.interval.ms";
    public static final long REPLICA_HIGH_WATERMARK_CHECKPOINT_INTERVAL_MS_DEFAULT = 5000L;
    public static final String REPLICA_HIGH_WATERMARK_CHECKPOINT_INTERVAL_MS_DOC = "The frequency with which the high watermark is saved out to disk";

    public static final String FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_CONFIG = "fetch.purgatory.purge.interval.requests";
    public static final int FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_DEFAULT = 1000;
    public static final String FETCH_PURGATORY_PURGE_INTERVAL_REQUESTS_DOC = "The purge interval (in number of requests) of the fetch request purgatory";

    public static final String PRODUCER_PURGATORY_PURGE_INTERVAL_REQUESTS_CONFIG = "producer.purgatory.purge.interval.requests";
    public static final int PRODUCER_PURGATORY_PURGE_INTERVAL_REQUESTS_DEFAULT = 1000;
    public static final String PRODUCER_PURGATORY_PURGE_INTERVAL_REQUESTS_DOC = "The purge interval (in number of requests) of the producer request purgatory";

    public static final String DELETE_RECORDS_PURGATORY_PURGE_INTERVAL_REQUESTS_CONFIG = "delete.records.purgatory.purge.interval.requests";
    public static final int DELETE_RECORDS_PURGATORY_PURGE_INTERVAL_REQUESTS_DEFAULT = 1;
    public static final String DELETE_RECORDS_PURGATORY_PURGE_INTERVAL_REQUESTS_DOC = "The purge interval (in number of requests) of the delete records request purgatory";

    public static final String LEADER_IMBALANCE_PER_BROKER_PERCENTAGE_CONFIG = "leader.imbalance.per.broker.percentage";
    public static final int LEADER_IMBALANCE_PER_BROKER_PERCENTAGE_DEFAULT = 10;
    public static final String LEADER_IMBALANCE_PER_BROKER_PERCENTAGE_DOC = "The ratio of leader imbalance allowed per broker. The controller would trigger a leader balance if it goes above this value per broker. The value is specified in percentage.";

    public static final String LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS_CONFIG = "leader.imbalance.check.interval.seconds";
    public static final int LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS_DEFAULT = 300;
    public static final String LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS_DOC = "The frequency with which the partition rebalance check is triggered by the controller";

    public static final String UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG);
    public static final String UNCLEAN_LEADER_ELECTION_ENABLE_DOC = "Indicates whether to enable replicas not in the ISR set to be elected as leader as a last resort, even though doing so may result in data loss";

    public static final String INTER_BROKER_PROTOCOL_VERSION_CONFIG = "inter.broker.protocol.version";
    public static final String INTER_BROKER_PROTOCOL_VERSION_DEFAULT = MetadataVersion.latestProduction().version();
    public static final String INTER_BROKER_PROTOCOL_VERSION_DOC = "Specify which version of the inter-broker protocol will be used.\n" +
           ". This is typically bumped after all brokers were upgraded to a new version.\n" +
           " Example of some valid values are: 0.8.0, 0.8.1, 0.8.1.1, 0.8.2, 0.8.2.0, 0.8.2.1, 0.9.0.0, 0.9.0.1 Check MetadataVersion for the full list.";

    public static final String INTER_BROKER_SECURITY_PROTOCOL_CONFIG = "security.inter.broker.protocol";
    public static final String INTER_BROKER_SECURITY_PROTOCOL_DEFAULT = SecurityProtocol.PLAINTEXT.toString();
    public static final String INTER_BROKER_LISTENER_NAME_CONFIG = "inter.broker.listener.name";
    public static final String INTER_BROKER_SECURITY_PROTOCOL_DOC = "Security protocol used to communicate between brokers. Valid values are: " +
            String.join(", ", SecurityProtocol.names()) + ". It is an error to set this and " + INTER_BROKER_LISTENER_NAME_CONFIG +
            " properties at the same time.";
    public static final String INTER_BROKER_LISTENER_NAME_DOC = "Name of listener used for communication between brokers. If this is unset, the listener name is defined by " + INTER_BROKER_SECURITY_PROTOCOL_CONFIG +
           "It is an error to set this and " + INTER_BROKER_SECURITY_PROTOCOL_CONFIG + " properties at the same time.";

    public static final String REPLICA_SELECTOR_CLASS_CONFIG = "replica.selector.class";
    public static final String REPLICA_SELECTOR_CLASS_DOC = "The fully qualified class name that implements ReplicaSelector. This is used by the broker to find the preferred read replica. By default, we use an implementation that returns the leader.";

    public static final String AUTO_LEADER_REBALANCE_ENABLE_CONFIG = "auto.leader.rebalance.enable";
    public static final boolean AUTO_LEADER_REBALANCE_ENABLE_DEFAULT = true;
    public static final String AUTO_LEADER_REBALANCE_ENABLE_DOC = String.format("Enables auto leader balancing. A background thread checks the distribution of partition leaders at regular intervals, configurable by %s. If the leader imbalance exceeds %s, leader rebalance to the preferred leader for partitions is triggered.",
           LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS_CONFIG, LEADER_IMBALANCE_PER_BROKER_PERCENTAGE_CONFIG);
}
