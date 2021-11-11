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

package org.apache.kafka.streams.kstream.internals.graph;

import org.apache.kafka.streams.kstream.internals.KTableKTableJoinMerger;
import org.apache.kafka.streams.kstream.internals.KTableProcessorSupplier;
import org.apache.kafka.streams.kstream.internals.KTableSource;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.internals.ProcessorAdapter;

/**
 * Class used to represent a {@link ProcessorSupplier} and the name
 * used to register it with the {@link org.apache.kafka.streams.processor.internals.InternalTopologyBuilder}
 *
 * Used by the Join nodes as there are several parameters, this abstraction helps
 * keep the number of arguments more reasonable.
 */
public class ProcessorParameters<KIn, VIn, KOut, VOut> {

    // During the transition to KIP-478, we capture arguments passed from the old API to simplify
    // the performance of casts that we still need to perform. This will eventually be removed.
    @SuppressWarnings("deprecation") // Old PAPI. Needs to be migrated.
    private final org.apache.kafka.streams.processor.ProcessorSupplier<KIn, VIn> oldProcessorSupplier;
    private final ProcessorSupplier<KIn, VIn, KOut, VOut> processorSupplier;
    private final String processorName;

    @SuppressWarnings("deprecation") // Old PAPI compatibility.
    public ProcessorParameters(final org.apache.kafka.streams.processor.ProcessorSupplier<KIn, VIn> processorSupplier,
                               final String processorName) {
        oldProcessorSupplier = processorSupplier;
        this.processorSupplier = () -> ProcessorAdapter.adapt(processorSupplier.get());
        this.processorName = processorName;
    }

    public ProcessorParameters(final ProcessorSupplier<KIn, VIn, KOut, VOut> processorSupplier,
                               final String processorName) {
        oldProcessorSupplier = null;
        this.processorSupplier = processorSupplier;
        this.processorName = processorName;
    }

    public ProcessorSupplier<KIn, VIn, KOut, VOut> processorSupplier() {
        return processorSupplier;
    }

    @SuppressWarnings("deprecation") // Old PAPI. Needs to be migrated.
    public org.apache.kafka.streams.processor.ProcessorSupplier<KIn, VIn> oldProcessorSupplier() {
        return oldProcessorSupplier;
    }

    @SuppressWarnings("unchecked")
    KTableSource<KIn, VIn> kTableSourceSupplier() {
        return processorSupplier instanceof KTableSource ? (KTableSource<KIn, VIn>) processorSupplier : null;
    }

    @SuppressWarnings("unchecked")
    <KR, VR> KTableProcessorSupplier<KIn, VIn, KR, VR> kTableProcessorSupplier() {
        // This cast always works because KTableProcessorSupplier hasn't been converted yet.
        return (KTableProcessorSupplier<KIn, VIn, KR, VR>) processorSupplier;
    }

    @SuppressWarnings("unchecked")
    KTableKTableJoinMerger<KIn, VIn> kTableKTableJoinMergerProcessorSupplier() {
        return (KTableKTableJoinMerger<KIn, VIn>) processorSupplier;
    }

    public String processorName() {
        return processorName;
    }

    @Override
    public String toString() {
        return "ProcessorParameters{" +
            "processor class=" + processorSupplier.get().getClass() +
            ", processor name='" + processorName + '\'' +
            '}';
    }
}
