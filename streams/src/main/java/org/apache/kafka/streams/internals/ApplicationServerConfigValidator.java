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
import org.apache.kafka.streams.state.HostInfo;

public class ApplicationServerConfigValidator implements ConfigDef.Validator {

    @Override
    public void ensureValid(final String name, final Object value) {
        if (!(value instanceof String)) {
            throw new ConfigException(name + " must be a string");
        }

        final String endPoint = (String) value;
        if (Utils.isBlank(endPoint)) {
            return;
        }
        try {
            HostInfo.buildFromEndpoint(endPoint);
        } catch (final ConfigException e) {
            throw new ConfigException(name, value, e.getMessage());
        }
    }

    @Override
    public String toString() {
        return "A host:port pair, protocol://host:port, or an empty string";
    }
}
