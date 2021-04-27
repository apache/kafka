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
package org.apache.kafka.clients;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Configurations shared by Kafka client applications: producer, consumer, connect, etc.
 */
public class CommonClientConfigs {
    private static final Logger log = LoggerFactory.getLogger(CommonClientConfigs.class);

    /*
     * NOTE: DO NOT CHANGE EITHER CONFIG NAMES AS THESE ARE PART OF THE PUBLIC API AND CHANGE WILL BREAK USER CODE.
     */

    public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
    public static final String BOOTSTRAP_SERVERS_DOC = "A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. The client will make use of all servers irrespective of which servers are specified here for bootstrapping&mdash;this list only impacts the initial hosts used to discover the full set of servers. This list should be in the form "
                                                       + "<code>host1:port1,host2:port2,...</code>. Since these servers are just used for the initial connection to "
                                                       + "discover the full cluster membership (which may change dynamically), this list need not contain the full set of "
                                                       + "servers (you may want more than one, though, in case a server is down).";

    public static final String CLIENT_DNS_LOOKUP_CONFIG = "client.dns.lookup";
    public static final String CLIENT_DNS_LOOKUP_DOC = "Controls how the client uses DNS lookups. "
                                                       + "If set to <code>use_all_dns_ips</code>, connect to each returned IP "
                                                       + "address in sequence until a successful connection is established. "
                                                       + "After a disconnection, the next IP is used. Once all IPs have been "
                                                       + "used once, the client resolves the IP(s) from the hostname again "
                                                       + "(both the JVM and the OS cache DNS name lookups, however). "
                                                       + "If set to <code>resolve_canonical_bootstrap_servers_only</code>, "
                                                       + "resolve each bootstrap address into a list of canonical names. After "
                                                       + "the bootstrap phase, this behaves the same as <code>use_all_dns_ips</code>.";

    public static final String METADATA_MAX_AGE_CONFIG = "metadata.max.age.ms";
    public static final String METADATA_MAX_AGE_DOC = "The period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any partition leadership changes to proactively discover any new brokers or partitions.";

    public static final String SEND_BUFFER_CONFIG = "send.buffer.bytes";
    public static final String SEND_BUFFER_DOC = "The size of the TCP send buffer (SO_SNDBUF) to use when sending data. If the value is -1, the OS default will be used.";
    public static final int SEND_BUFFER_LOWER_BOUND = -1;

    public static final String RECEIVE_BUFFER_CONFIG = "receive.buffer.bytes";
    public static final String RECEIVE_BUFFER_DOC = "The size of the TCP receive buffer (SO_RCVBUF) to use when reading data. If the value is -1, the OS default will be used.";
    public static final int RECEIVE_BUFFER_LOWER_BOUND = -1;

    public static final String CLIENT_ID_CONFIG = "client.id";
    public static final String CLIENT_ID_DOC = "An id string to pass to the server when making requests. The purpose of this is to be able to track the source of requests beyond just ip/port by allowing a logical application name to be included in server-side request logging.";

    public static final String CLIENT_RACK_CONFIG = "client.rack";
    public static final String CLIENT_RACK_DOC = "A rack identifier for this client. This can be any string value which indicates where this client is physically located. It corresponds with the broker config 'broker.rack'";

    public static final String RECONNECT_BACKOFF_MS_CONFIG = "reconnect.backoff.ms";
    public static final String RECONNECT_BACKOFF_MS_DOC = "The base amount of time to wait before attempting to reconnect to a given host. This avoids repeatedly connecting to a host in a tight loop. This backoff applies to all connection attempts by the client to a broker.";

    public static final String RECONNECT_BACKOFF_MAX_MS_CONFIG = "reconnect.backoff.max.ms";
    public static final String RECONNECT_BACKOFF_MAX_MS_DOC = "The maximum amount of time in milliseconds to wait when reconnecting to a broker that has repeatedly failed to connect. If provided, the backoff per host will increase exponentially for each consecutive connection failure, up to this maximum. After calculating the backoff increase, 20% random jitter is added to avoid connection storms.";

    public static final String RETRIES_CONFIG = "retries";
    public static final String RETRIES_DOC = "Setting a value greater than zero will cause the client to resend any request that fails with a potentially transient error." +
        " It is recommended to set the value to either zero or `MAX_VALUE` and use corresponding timeout parameters to control how long a client should retry a request.";

    public static final String RETRY_BACKOFF_MS_CONFIG = "retry.backoff.ms";
    public static final String RETRY_BACKOFF_MS_DOC = "The amount of time to wait before attempting to retry a failed request to a given topic partition. This avoids repeatedly sending requests in a tight loop under some failure scenarios.";

    public static final String METRICS_SAMPLE_WINDOW_MS_CONFIG = "metrics.sample.window.ms";
    public static final String METRICS_SAMPLE_WINDOW_MS_DOC = "The window of time a metrics sample is computed over.";

    public static final String METRICS_NUM_SAMPLES_CONFIG = "metrics.num.samples";
    public static final String METRICS_NUM_SAMPLES_DOC = "The number of samples maintained to compute metrics.";

    public static final String METRICS_RECORDING_LEVEL_CONFIG = "metrics.recording.level";
    public static final String METRICS_RECORDING_LEVEL_DOC = "The highest recording level for metrics.";

    public static final String METRIC_REPORTER_CLASSES_CONFIG = "metric.reporters";
    public static final String METRIC_REPORTER_CLASSES_DOC = "A list of classes to use as metrics reporters. Implementing the <code>org.apache.kafka.common.metrics.MetricsReporter</code> interface allows plugging in classes that will be notified of new metric creation. The JmxReporter is always included to register JMX statistics.";

    public static final String METRICS_CONTEXT_PREFIX = "metrics.context.";

    public static final String SECURITY_PROTOCOL_CONFIG = "security.protocol";
    public static final String SECURITY_PROTOCOL_DOC = "Protocol used to communicate with brokers. Valid values are: " +
        Utils.join(SecurityProtocol.names(), ", ") + ".";
    public static final String DEFAULT_SECURITY_PROTOCOL = "PLAINTEXT";

    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG = "socket.connection.setup.timeout.ms";
    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC = "The amount of time the client will wait for the socket connection to be established. If the connection is not built before the timeout elapses, clients will close the socket channel.";
    public static final Long DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS = 10 * 1000L;

    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG = "socket.connection.setup.timeout.max.ms";
    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC = "The maximum amount of time the client will wait for the socket connection to be established. The connection setup timeout will increase exponentially for each consecutive connection failure up to this maximum. To avoid connection storms, a randomization factor of 0.2 will be applied to the timeout resulting in a random range between 20% below and 20% above the computed value.";
    public static final Long DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS = 30 * 1000L;

    public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = "connections.max.idle.ms";
    public static final String CONNECTIONS_MAX_IDLE_MS_DOC = "Close idle connections after the number of milliseconds specified by this config.";

    public static final String REQUEST_TIMEOUT_MS_CONFIG = "request.timeout.ms";
    public static final String REQUEST_TIMEOUT_MS_DOC = "The configuration controls the maximum amount of time the client will wait "
                                                         + "for the response of a request. If the response is not received before the timeout "
                                                         + "elapses the client will resend the request if necessary or fail the request if "
                                                         + "retries are exhausted.";

    public static final String GROUP_ID_CONFIG = "group.id";
    public static final String GROUP_ID_DOC = "A unique string that identifies the consumer group this consumer belongs to. This property is required if the consumer uses either the group management functionality by using <code>subscribe(topic)</code> or the Kafka-based offset management strategy.";

    public static final String GROUP_INSTANCE_ID_CONFIG = "group.instance.id";
    public static final String GROUP_INSTANCE_ID_DOC = "A unique identifier of the consumer instance provided by the end user. "
                                                       + "Only non-empty strings are permitted. If set, the consumer is treated as a static member, "
                                                       + "which means that only one instance with this ID is allowed in the consumer group at any time. "
                                                       + "This can be used in combination with a larger session timeout to avoid group rebalances caused by transient unavailability "
                                                       + "(e.g. process restarts). If not set, the consumer will join the group as a dynamic member, which is the traditional behavior.";

    public static final String MAX_POLL_INTERVAL_MS_CONFIG = "max.poll.interval.ms";
    public static final String MAX_POLL_INTERVAL_MS_DOC = "The maximum delay between invocations of poll() when using "
                                                          + "consumer group management. This places an upper bound on the amount of time that the consumer can be idle "
                                                          + "before fetching more records. If poll() is not called before expiration of this timeout, then the consumer "
                                                          + "is considered failed and the group will rebalance in order to reassign the partitions to another member. "
                                                          + "For consumers using a non-null <code>group.instance.id</code> which reach this timeout, partitions will not be immediately reassigned. "
                                                          + "Instead, the consumer will stop sending heartbeats and partitions will be reassigned "
                                                          + "after expiration of <code>session.timeout.ms</code>. This mirrors the behavior of a static consumer which has shutdown.";

    public static final String REBALANCE_TIMEOUT_MS_CONFIG = "rebalance.timeout.ms";
    public static final String REBALANCE_TIMEOUT_MS_DOC = "The maximum allowed time for each worker to join the group "
                                                          + "once a rebalance has begun. This is basically a limit on the amount of time needed for all tasks to "
                                                          + "flush any pending data and commit offsets. If the timeout is exceeded, then the worker will be removed "
                                                          + "from the group, which will cause offset commit failures.";

    public static final String SESSION_TIMEOUT_MS_CONFIG = "session.timeout.ms";
    public static final String SESSION_TIMEOUT_MS_DOC = "The timeout used to detect client failures when using "
                                                        + "Kafka's group management facility. The client sends periodic heartbeats to indicate its liveness "
                                                        + "to the broker. If no heartbeats are received by the broker before the expiration of this session timeout, "
                                                        + "then the broker will remove this client from the group and initiate a rebalance. Note that the value "
                                                        + "must be in the allowable range as configured in the broker configuration by <code>group.min.session.timeout.ms</code> "
                                                        + "and <code>group.max.session.timeout.ms</code>.";

    public static final String HEARTBEAT_INTERVAL_MS_CONFIG = "heartbeat.interval.ms";
    public static final String HEARTBEAT_INTERVAL_MS_DOC = "The expected time between heartbeats to the consumer "
                                                           + "coordinator when using Kafka's group management facilities. Heartbeats are used to ensure that the "
                                                           + "consumer's session stays active and to facilitate rebalancing when new consumers join or leave the group. "
                                                           + "The value must be set lower than <code>session.timeout.ms</code>, but typically should be set no higher "
                                                           + "than 1/3 of that value. It can be adjusted even lower to control the expected time for normal rebalances.";

    public static final String DEFAULT_API_TIMEOUT_MS_CONFIG = "default.api.timeout.ms";
    public static final String DEFAULT_API_TIMEOUT_MS_DOC = "Specifies the timeout (in milliseconds) for client APIs. " +
            "This configuration is used as the default timeout for all client operations that do not specify a <code>timeout</code> parameter.";

    /**
     * Postprocess the configuration so that exponential backoff is disabled when reconnect backoff
     * is explicitly configured but the maximum reconnect backoff is not explicitly configured.
     *
     * @param config                    The config object.
     * @param parsedValues              The parsedValues as provided to postProcessParsedConfig.
     *
     * @return                          The new values which have been set as described in postProcessParsedConfig.
     */
    public static Map<String, Object> postProcessReconnectBackoffConfigs(AbstractConfig config,
                                                    Map<String, Object> parsedValues) {
        HashMap<String, Object> rval = new HashMap<>();
        if ((!config.originals().containsKey(RECONNECT_BACKOFF_MAX_MS_CONFIG)) &&
                config.originals().containsKey(RECONNECT_BACKOFF_MS_CONFIG)) {
            log.debug("Disabling exponential reconnect backoff because {} is set, but {} is not.",
                    RECONNECT_BACKOFF_MS_CONFIG, RECONNECT_BACKOFF_MAX_MS_CONFIG);
            rval.put(RECONNECT_BACKOFF_MAX_MS_CONFIG, parsedValues.get(RECONNECT_BACKOFF_MS_CONFIG));
        }
        return rval;
    }
}
