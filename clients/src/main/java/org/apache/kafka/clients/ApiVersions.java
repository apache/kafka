/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.apache.kafka.clients;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.LogEntry;

import java.util.HashMap;
import java.util.Map;

/**
 * Maintains node api versions for access to api versions without dependence on the NetworkClient
 * (which is the source of the information). The pattern is akin to the use of {@link Metadata} for
 * topic metadata.
 */
public class ApiVersions {

    private final Map<String, NodeApiVersions> nodeApiVersions = new HashMap<>();
    private byte maxUsableProduceMagic = LogEntry.CURRENT_MAGIC_VALUE;

    public synchronized void update(String nodeId, NodeApiVersions nodeApiVersions) {
        this.nodeApiVersions.put(nodeId, nodeApiVersions);
        this.maxUsableProduceMagic = computeMaxUsableProduceMagic();
    }

    public synchronized void remove(String nodeId) {
        this.nodeApiVersions.remove(nodeId);
        this.maxUsableProduceMagic = computeMaxUsableProduceMagic();
    }

    public synchronized NodeApiVersions get(String nodeId) {
        return this.nodeApiVersions.get(nodeId);
    }

    private byte computeMaxUsableProduceMagic() {
        // use a magic version which is supported by all brokers to reduce the chance that
        // we will need to convert the messages when they are ready to be sent.

        byte maxUsableMagic = LogEntry.CURRENT_MAGIC_VALUE;
        for (NodeApiVersions versions : this.nodeApiVersions.values()) {
            byte usableMagic;
            switch (versions.usableVersion(ApiKeys.PRODUCE)) {
                case 0:
                case 1:
                    usableMagic = LogEntry.MAGIC_VALUE_V0;
                    break;

                case 2:
                    usableMagic = LogEntry.MAGIC_VALUE_V1;
                    break;

                default:
                    usableMagic = LogEntry.MAGIC_VALUE_V2;
            }
            if (usableMagic < maxUsableMagic)
                maxUsableMagic = usableMagic;
        }
        return maxUsableMagic;
    }

    public synchronized byte maxUsableProduceMagic() {
        return maxUsableProduceMagic;
    }

}
