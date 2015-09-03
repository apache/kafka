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

package org.apache.kafka.common.network;

import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.security.ssl.SSLFactory;

import java.util.Map;

public class ChannelBuilders {

    private ChannelBuilders() { }

    /**
     * @param securityProtocol the securityProtocol
     * @param mode the SSL mode, it must be non-null if `securityProcol` is `SSL` and it is ignored otherwise
     * @param configs client/server configs
     * @return the configured `ChannelBuilder`
     * @throws IllegalArgumentException if `mode` invariants described above is not maintained
     */
    public static ChannelBuilder create(SecurityProtocol securityProtocol, SSLFactory.Mode mode, Map<String, ?> configs) {
        ChannelBuilder channelBuilder = null;

        switch (securityProtocol) {
            case SSL:
                if (mode == null)
                    throw new IllegalArgumentException("`mode` must be non-null if `securityProtocol` is `SSL`");
                channelBuilder = new SSLChannelBuilder(mode);
                break;
            case PLAINTEXT:
            case TRACE:
                channelBuilder = new PlaintextChannelBuilder();
                break;
            default:
                throw new IllegalArgumentException("Unexpected securityProtocol " + securityProtocol);
        }

        channelBuilder.configure(configs);
        return channelBuilder;
    }
}
