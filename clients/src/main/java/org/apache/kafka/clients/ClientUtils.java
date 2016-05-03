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

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.LoginType;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.common.utils.Utils.getHost;
import static org.apache.kafka.common.utils.Utils.getPort;

public class ClientUtils {
    private static final Logger log = LoggerFactory.getLogger(ClientUtils.class);

    public static List<InetSocketAddress> parseAndValidateAddresses(List<String> urls) {
        List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
        for (String url : urls) {
            if (url != null && url.length() > 0) {
                String host = getHost(url);
                Integer port = getPort(url);
                if (host == null || port == null)
                    throw new ConfigException("Invalid url in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG + ": " + url);
                try {
                    InetSocketAddress address = new InetSocketAddress(host, port);
                    if (address.isUnresolved())
                        throw new ConfigException("DNS resolution failed for url in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG + ": " + url);
                    addresses.add(address);
                } catch (NumberFormatException e) {
                    throw new ConfigException("Invalid port in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG + ": " + url);
                }
            }
        }
        if (addresses.size() < 1)
            throw new ConfigException("No bootstrap urls given in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
        return addresses;
    }

    public static void closeQuietly(Closeable c, String name, AtomicReference<Throwable> firstException) {
        if (c != null) {
            try {
                c.close();
            } catch (Throwable t) {
                firstException.compareAndSet(null, t);
                log.error("Failed to close " + name, t);
            }
        }
    }

    /**
     * @param configs client/server configs
     * @return configured ChannelBuilder based on the configs.
     */
    public static ChannelBuilder createChannelBuilder(Map<String, ?> configs) {
        SecurityProtocol securityProtocol = SecurityProtocol.forName((String) configs.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        if (!SecurityProtocol.nonTestingValues().contains(securityProtocol))
            throw new ConfigException("Invalid SecurityProtocol " + securityProtocol);
        String clientSaslMechanism = (String) configs.get(SaslConfigs.SASL_MECHANISM);
        return ChannelBuilders.create(securityProtocol, Mode.CLIENT, LoginType.CLIENT, configs, clientSaslMechanism, true);
    }

}
