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
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.kafka.common.utils.Utils.getHost;
import static org.apache.kafka.common.utils.Utils.getPort;

public final class ClientUtils {
    private static final Logger log = LoggerFactory.getLogger(ClientUtils.class);

    private ClientUtils() {
    }

    public static List<InetSocketAddress> parseAndValidateAddresses(List<String> urls, String clientDnsLookupConfig) {
        return parseAndValidateAddresses(urls, ClientDnsLookup.forConfig(clientDnsLookupConfig));
    }

    public static List<InetSocketAddress> parseAndValidateAddresses(List<String> urls, ClientDnsLookup clientDnsLookup) {
        List<InetSocketAddress> addresses = new ArrayList<>();
        for (String url : urls) {
            if (url != null && !url.isEmpty()) {
                try {
                    String host = getHost(url);
                    Integer port = getPort(url);
                    if (host == null || port == null)
                        throw new ConfigException("Invalid url in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG + ": " + url);

                    if (clientDnsLookup == ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY) {
                        InetAddress[] inetAddresses = DefaultDnsNameResolver.DEFAULT_DNS_NAME_RESOLVER.resolve(host);
                        for (InetAddress inetAddress : inetAddresses) {
                            String resolvedCanonicalName = inetAddress.getCanonicalHostName();
                            InetSocketAddress address = new InetSocketAddress(resolvedCanonicalName, port);
                            if (address.isUnresolved()) {
                                log.warn("Couldn't resolve server {} from {} as DNS resolution of the canonical hostname {} failed for {}", url, CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, resolvedCanonicalName, host);
                            } else {
                                addresses.add(address);
                            }
                        }
                    } else {
                        InetSocketAddress address = new InetSocketAddress(host, port);
                        if (address.isUnresolved()) {
                            log.warn("Couldn't resolve server {} from {} as DNS resolution failed for {}", url, CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, host);
                        } else {
                            addresses.add(address);
                        }
                    }

                } catch (IllegalArgumentException e) {
                    throw new ConfigException("Invalid port in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG + ": " + url);
                } catch (UnknownHostException e) {
                    throw new ConfigException("Unknown host in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG + ": " + url);
                }
            }
        }
        if (addresses.isEmpty())
            throw new ConfigException("No resolvable bootstrap urls given in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
        return addresses;
    }

    /**
     * Create a new channel builder from the provided configuration.
     *
     * @param config client configs
     * @param time the time implementation
     * @param logContext the logging context
     *
     * @return configured ChannelBuilder based on the configs.
     */
    public static ChannelBuilder createChannelBuilder(AbstractConfig config, Time time, LogContext logContext) {
        SecurityProtocol securityProtocol = SecurityProtocol.forName(config.getString(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        String clientSaslMechanism = config.getString(SaslConfigs.SASL_MECHANISM);
        return ChannelBuilders.clientChannelBuilder(securityProtocol, JaasContext.Type.CLIENT, config, null,
                clientSaslMechanism, time, true, logContext);
    }

    static List<InetAddress> resolve(String host,
                                     DnsNameResolver dnsNameResolver,
                                     ClientDnsLookup clientDnsLookup) throws UnknownHostException {
        InetAddress[] addresses = dnsNameResolver.resolve(host);

        switch (clientDnsLookup) {
            case DEFAULT:
                return Collections.singletonList(addresses[0]);
            case USE_ALL_DNS_IPS:
            case RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY:
                return filterPreferredAddresses(addresses);
        }

        throw new IllegalStateException("Unhandled ClientDnsLookup instance: " + clientDnsLookup);
    }

    /**
     * Return a list containing the first address in `allAddresses` and subsequent addresses
     * that are a subtype of the first address.
     *
     * The outcome is that all returned addresses are either IPv4 or IPv6 (InetAddress has two
     * subclasses: Inet4Address and Inet6Address).
     */
    static List<InetAddress> filterPreferredAddresses(InetAddress[] allAddresses) {
        List<InetAddress> preferredAddresses = new ArrayList<>();
        Class<? extends InetAddress> clazz = null;
        for (InetAddress address : allAddresses) {
            if (clazz == null) {
                clazz = address.getClass();
            }
            if (clazz.isInstance(address)) {
                preferredAddresses.add(address);
            }
        }
        return preferredAddresses;
    }
}
