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
 * Some configurations shared by both producer and consumer
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
    public static final String CLIENT_DNS_LOOKUP_DOC = "<p>Controls how the client uses DNS lookups.</p><p>If set to <code>use_all_dns_ips</code> then, when the lookup returns multiple IP addresses for a hostname,"
            + " they will all be attempted to connect to before failing the connection. Applies to both bootstrap and advertised servers.</p>"
            + "<p>If the value is <code>resolve_canonical_bootstrap_servers_only</code> each entry will be resolved and expanded into a list of canonical names.</p>";

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

    public static final String RECONNECT_BACKOFF_MS_CONFIG = "reconnect.backoff.ms";
    public static final String RECONNECT_BACKOFF_MS_DOC = "The base amount of time to wait before attempting to reconnect to a given host. This avoids repeatedly connecting to a host in a tight loop. This backoff applies to all connection attempts by the client to a broker.";

    public static final String RECONNECT_BACKOFF_MAX_MS_CONFIG = "reconnect.backoff.max.ms";
    public static final String RECONNECT_BACKOFF_MAX_MS_DOC = "The maximum amount of time in milliseconds to wait when reconnecting to a broker that has repeatedly failed to connect. If provided, the backoff per host will increase exponentially for each consecutive connection failure, up to this maximum. After calculating the backoff increase, 20% random jitter is added to avoid connection storms.";

    public static final String RETRIES_CONFIG = "retries";
    public static final String RETRIES_DOC = "Setting a value greater than zero will cause the client to resend any request that fails with a potentially transient error.";

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

    public static final String SECURITY_PROTOCOL_CONFIG = "security.protocol";
    public static final String SECURITY_PROTOCOL_DOC = "Protocol used to communicate with brokers. Valid values are: " +
        Utils.join(SecurityProtocol.names(), ", ") + ".";
    public static final String DEFAULT_SECURITY_PROTOCOL = "PLAINTEXT";

    public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = "connections.max.idle.ms";
    public static final String CONNECTIONS_MAX_IDLE_MS_DOC = "Close idle connections after the number of milliseconds specified by this config.";

    public static final String REQUEST_TIMEOUT_MS_CONFIG = "request.timeout.ms";
    public static final String REQUEST_TIMEOUT_MS_DOC = "The configuration controls the maximum amount of time the client will wait "
                                                         + "for the response of a request. If the response is not received before the timeout "
                                                         + "elapses the client will resend the request if necessary or fail the request if "
                                                         + "retries are exhausted.";

    public static final String DEFAULT_CONNECT_READY_TIMEOUT_MS_CONFIG = "connections.ready.timeout.ms";
    public static final String DEFAULT_CONNECT_READY_TIMEOUT_MS_DOC = "default timeout for the connection can not recieve the ApiVersionsResponse from servers. if in this time the connection is not ready,then close the connection and trigger update the metaData.";


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
