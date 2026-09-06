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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

import java.util.List;

public final class BootstrapConfiguration {
    public static final BootstrapConfiguration DISABLED =
        new BootstrapConfiguration(List.of(), null, 0, 0);

    public final List<String> bootstrapServers;
    public final ClientDnsLookup clientDnsLookup;
    public final long bootstrapResolveTimeoutMs;
    public final long retryBackoffMs;

    private BootstrapConfiguration(final List<String> bootstrapServers,
                                   final ClientDnsLookup clientDnsLookup,
                                   final long bootstrapResolveTimeoutMs,
                                   final long retryBackoffMs) {
        this.bootstrapServers = bootstrapServers;
        this.clientDnsLookup = clientDnsLookup;
        this.bootstrapResolveTimeoutMs = bootstrapResolveTimeoutMs;
        this.retryBackoffMs = retryBackoffMs;
    }

    public static BootstrapConfiguration enabled(final List<String> bootstrapServers,
                                                 final ClientDnsLookup clientDnsLookup,
                                                 final long bootstrapResolveTimeoutMs,
                                                 final long retryBackoffMs) {
        for (String url : bootstrapServers) {
            try {
                if (Utils.getHost(url) == null || Utils.getPort(url) == null)
                    throw new ConfigException("Invalid url in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG + ": " + url);
            } catch (IllegalArgumentException e) {
                throw new ConfigException("Invalid port in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG + ": " + url);
            }
        }
        return new BootstrapConfiguration(bootstrapServers, clientDnsLookup, bootstrapResolveTimeoutMs, retryBackoffMs);
    }
}
