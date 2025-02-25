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

package org.apache.kafka.network;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.util.Locale;

public record EndPoint(
        String host,
        int port,
        ListenerName listenerName,
        SecurityProtocol securityProtocol
) {
    public static String parseListenerName(String connectionString) {
        int firstColon = connectionString.indexOf(':');
        if (firstColon < 0) {
            throw new KafkaException("Unable to parse a listener name from " + connectionString);
        }
        return connectionString.substring(0, firstColon).toUpperCase(Locale.ROOT);
    }

    public static EndPoint fromPublic(org.apache.kafka.common.Endpoint endpoint) {
        return new EndPoint(endpoint.host(), endpoint.port(),
                new ListenerName(endpoint.listenerName().get()), endpoint.securityProtocol());
    }

    public org.apache.kafka.common.Endpoint toPublic() {
        return new org.apache.kafka.common.Endpoint(listenerName.value(), securityProtocol, host, port);
    }
}