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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.streams.processor.StateStore;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public final class ProcessorTopologyFactories {
    private ProcessorTopologyFactories() {}


    public static ProcessorTopology with(final List<ProcessorNode> processorNodes,
                                         final Map<String, SourceNode> sourcesByTopic,
                                         final List<StateStore> stateStoresByName,
                                         final Map<String, String> storeToChangelogTopic) {
        return new ProcessorTopology(processorNodes,
                                     sourcesByTopic,
                                     Collections.emptyMap(),
                                     stateStoresByName,
                                     Collections.emptyList(),
                                     storeToChangelogTopic,
                                     Collections.emptySet());
    }

    static ProcessorTopology withLocalStores(final List<StateStore> stateStores,
                                             final Map<String, String> storeToChangelogTopic) {
        return new ProcessorTopology(Collections.emptyList(),
                                     Collections.emptyMap(),
                                     Collections.emptyMap(),
                                     stateStores,
                                     Collections.emptyList(),
                                     storeToChangelogTopic,
                                     Collections.emptySet());
    }

}
