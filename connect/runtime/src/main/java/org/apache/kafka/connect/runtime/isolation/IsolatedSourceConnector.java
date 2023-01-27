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
package org.apache.kafka.connect.runtime.isolation;

import org.apache.kafka.connect.source.ConnectorTransactionBoundaries;
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.Map;

public class IsolatedSourceConnector extends IsolatedConnector<SourceConnector> {

    IsolatedSourceConnector(Plugins plugins, SourceConnector delegate) {
        super(plugins, delegate, PluginType.SOURCE);
    }

    public ExactlyOnceSupport exactlyOnceSupport(Map<String, String> connectorConfig) throws Exception {
        return isolate(delegate::exactlyOnceSupport, connectorConfig);
    }

    public ConnectorTransactionBoundaries canDefineTransactionBoundaries(Map<String, String> connectorConfig) throws Exception {
        return isolate(delegate::canDefineTransactionBoundaries, connectorConfig);
    }

}
