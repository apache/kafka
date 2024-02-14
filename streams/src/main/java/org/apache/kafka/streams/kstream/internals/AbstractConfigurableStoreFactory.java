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
package org.apache.kafka.streams.kstream.internals;

import java.util.HashSet;
import java.util.Set;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.StoreFactory;
import org.apache.kafka.streams.state.DslStoreSuppliers;

public abstract class AbstractConfigurableStoreFactory implements StoreFactory {
    private final Set<String> connectedProcessorNames = new HashSet<>();
    private DslStoreSuppliers dslStoreSuppliers;

    public AbstractConfigurableStoreFactory(final DslStoreSuppliers initialStoreSuppliers) {
        this.dslStoreSuppliers = initialStoreSuppliers;
    }

    @Override
    public void configure(final StreamsConfig config) {
        if (dslStoreSuppliers == null) {
            dslStoreSuppliers = Utils.newInstance(
                    config.getClass(StreamsConfig.DSL_STORE_SUPPLIERS_CLASS_CONFIG),
                    DslStoreSuppliers.class);
        }
    }

    @Override
    public Set<String> connectedProcessorNames() {
        return connectedProcessorNames;
    }

    protected DslStoreSuppliers dslStoreSuppliers() {
        if (dslStoreSuppliers == null) {
            throw new IllegalStateException("Expected configure() to be called before using dslStoreSuppliers");
        }
        return dslStoreSuppliers;
    }
}
