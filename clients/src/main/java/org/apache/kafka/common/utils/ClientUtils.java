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
package org.apache.kafka.common.utils;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigException;

import static org.apache.kafka.common.utils.Utils.getHost;
import static org.apache.kafka.common.utils.Utils.getPort;

public class ClientUtils {

    public static List<InetSocketAddress> parseAndValidateAddresses(List<String> urls) {
        List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
        for (String url : urls) {
            if (url != null && url.length() > 0) {
                String host = getHost(url);
                Integer port = getPort(url);
                if (host == null || port == null)
                    throw new ConfigException("Invalid url in " + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG + ": " + url);
                try {
                    InetSocketAddress address = new InetSocketAddress(host, port);
                    if (address.isUnresolved())
                        throw new ConfigException("DNS resolution failed for url in " + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG + ": " + url);
                    addresses.add(address);
                } catch (NumberFormatException e) {
                    throw new ConfigException("Invalid port in " + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG + ": " + url);
                }
            }
        }
        if (addresses.size() < 1)
            throw new ConfigException("No bootstrap urls given in " + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
        return addresses;
    }
}