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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.StateStoreContext;

/**
 * Allows serde access across different context types.
 */
public class SerdeGetter {

    private final org.apache.kafka.streams.processor.ProcessorContext oldProcessorContext;
    private final org.apache.kafka.streams.processor.api.ProcessorContext newProcessorContext;
    private final StateStoreContext stateStorecontext;
    public SerdeGetter(final org.apache.kafka.streams.processor.ProcessorContext context) {
        oldProcessorContext = context;
        newProcessorContext = null;
        stateStorecontext = null;
    }
    public SerdeGetter(final org.apache.kafka.streams.processor.api.ProcessorContext context) {
        oldProcessorContext = null;
        newProcessorContext = context;
        stateStorecontext = null;
    }
    public SerdeGetter(final StateStoreContext context) {
        oldProcessorContext = null;
        newProcessorContext = null;
        stateStorecontext = context;
    }
    public Serde keySerde() {
        return oldProcessorContext != null ? oldProcessorContext.keySerde() :
            newProcessorContext != null ? newProcessorContext.keySerde() : stateStorecontext.keySerde();
    }
    public Serde valueSerde() {
        return oldProcessorContext != null ? oldProcessorContext.valueSerde() :
            newProcessorContext != null ? newProcessorContext.valueSerde() : stateStorecontext.valueSerde();
    }

}
