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
package org.apache.kafka.streams.internals;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

import static org.apache.kafka.common.utils.Utils.getHost;
import static org.apache.kafka.common.utils.Utils.getPort;

public class ApplicationServerConfigValidator implements ConfigDef.Validator {

    private static final ConfigDef.Validator INSTANCE = new ApplicationServerConfigValidator();

    public static ConfigDef.Validator getInstance() {
        return INSTANCE;
    }

    @Override
    public void ensureValid(final String name, final Object value) {
        if (!(value instanceof String)) {
            throw new ConfigException(name + " must be a string");
        }

        final String endPoint = (String) value;

        if (endPoint.isEmpty()) {
            return;
        }

        if (Utils.isBlank(endPoint)) {
            return;
        }

        final String host = getHost(endPoint);
        final Integer port;
        try {
            port = getPort(endPoint);
        } catch (final NumberFormatException e) {
            throw new ConfigException(name, value, "Invalid port: " + e.getMessage());
        }

        if (host == null || port == null) {
            throw new ConfigException(
                    name, value, String.format("Error parsing host address %s. Expected format host:port.", endPoint)
            );
        }
    }

    @Override
    public String toString() {
        return "A host:port pair, protocol://host:port, or an empty string";
    }
}
