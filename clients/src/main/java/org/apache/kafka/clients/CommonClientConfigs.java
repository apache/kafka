/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.kafka.clients;

import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;

import java.util.ArrayList;
import java.util.List;

/**
 * Some configurations shared by both producer and consumer
 */
public class CommonClientConfigs {

    /*
     * NOTE: DO NOT CHANGE EITHER CONFIG NAMES AS THESE ARE PART OF THE PUBLIC API AND CHANGE WILL BREAK USER CODE.
     */

    public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
    public static final String BOOSTRAP_SERVERS_DOC = "A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. The client will make use of all servers irrespective of which servers are specified here for bootstrapping&mdash;this list only impacts the initial hosts used to discover the full set of servers. This list should be in the form "
                                                       + "<code>host1:port1,host2:port2,...</code>. Since these servers are just used for the initial connection to "
                                                       + "discover the full cluster membership (which may change dynamically), this list need not contain the full set of "
                                                       + "servers (you may want more than one, though, in case a server is down).";

    public static final String METADATA_MAX_AGE_CONFIG = "metadata.max.age.ms";
    public static final String METADATA_MAX_AGE_DOC = "The period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any partition leadership changes to proactively discover any new brokers or partitions.";

    public static final String SEND_BUFFER_CONFIG = "send.buffer.bytes";
    public static final String SEND_BUFFER_DOC = "The size of the TCP send buffer (SO_SNDBUF) to use when sending data.";

    public static final String RECEIVE_BUFFER_CONFIG = "receive.buffer.bytes";
    public static final String RECEIVE_BUFFER_DOC = "The size of the TCP receive buffer (SO_RCVBUF) to use when reading data.";

    public static final String CLIENT_ID_CONFIG = "client.id";
    public static final String CLIENT_ID_DOC = "An id string to pass to the server when making requests. The purpose of this is to be able to track the source of requests beyond just ip/port by allowing a logical application name to be included in server-side request logging.";

    public static final String RECONNECT_BACKOFF_MS_CONFIG = "reconnect.backoff.ms";
    public static final String RECONNECT_BACKOFF_MS_DOC = "The amount of time to wait before attempting to reconnect to a given host. This avoids repeatedly connecting to a host in a tight loop. This backoff applies to all requests sent by the consumer to the broker.";

    public static final String RETRY_BACKOFF_MS_CONFIG = "retry.backoff.ms";
    public static final String RETRY_BACKOFF_MS_DOC = "The amount of time to wait before attempting to retry a failed request to a given topic partition. This avoids repeatedly sending requests in a tight loop under some failure scenarios.";

    public static final String METRICS_SAMPLE_WINDOW_MS_CONFIG = "metrics.sample.window.ms";
    public static final String METRICS_SAMPLE_WINDOW_MS_DOC = "The window of time a metrics sample is computed over.";

    public static final String METRICS_NUM_SAMPLES_CONFIG = "metrics.num.samples";
    public static final String METRICS_NUM_SAMPLES_DOC = "The number of samples maintained to compute metrics.";

    public static final String METRIC_REPORTER_CLASSES_CONFIG = "metric.reporters";
    public static final String METRIC_REPORTER_CLASSES_DOC = "A list of classes to use as metrics reporters. Implementing the <code>MetricReporter</code> interface allows plugging in classes that will be notified of new metric creation. The JmxReporter is always included to register JMX statistics.";

    public static final String SECURITY_PROTOCOL_CONFIG = "security.protocol";
    public static final String SECURITY_PROTOCOL_DOC = "Protocol used to communicate with brokers. Valid values are: " + Utils.mkString(nonTestingSecurityProtocolNames(), ", ") + ".";
    public static final String DEFAULT_SECURITY_PROTOCOL = "PLAINTEXT";

    public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = "connections.max.idle.ms";
    public static final String CONNECTIONS_MAX_IDLE_MS_DOC = "Close idle connections after the number of milliseconds specified by this config.";

    public static final String REQUEST_TIMEOUT_MS_CONFIG = "request.timeout.ms";
    public static final String REQUEST_TIMEOUT_MS_DOC = "The configuration controls the maximum amount of time the client will wait "
                                                         + "for the response of a request. If the response is not received before the timeout "
                                                         + "elapses the client will resend the request if necessary or fail the request if "
                                                         + "retries are exhausted.";

    private static List<String> nonTestingSecurityProtocolNames() {
        List<String> names = new ArrayList<>();
        for (SecurityProtocol protocol : SecurityProtocol.nonTestingValues())
            names.add(protocol.name);
        return names;
    }

}
