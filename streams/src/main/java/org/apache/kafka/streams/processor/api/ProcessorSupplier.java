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
package org.apache.kafka.streams.processor.api;

import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.ConnectedStoreProvider;

/**
 * A processor supplier that can create one or more {@link Processor} instances.
 *
 * It is used in {@link Topology} for adding new processor operators, whose generated
 * topology can then be replicated (and thus creating one or more {@link Processor} instances)
 * and distributed to multiple stream threads.
 *
 * @param <KIn> the type of input keys
 * @param <VIn> the type of input values
 * @param <KOut> the type of output keys
 * @param <VOut> the type of output values
 */
@FunctionalInterface
public interface ProcessorSupplier<KIn, VIn, KOut, VOut> extends ConnectedStoreProvider {

    /**
     * Return a new {@link Processor} instance.
     *
     * @return a new {@link Processor} instance
     */
    Processor<KIn, VIn, KOut, VOut> get();
}
