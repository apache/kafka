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

import org.apache.kafka.streams.kstream.internals.KTableChangeProcessorSupplier;
import org.apache.kafka.streams.kstream.internals.KTableKTableJoinMerger;
import org.apache.kafka.streams.kstream.internals.KTableSource;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

/**
 * Class used to represent a {@link ProcessorSupplier} and the name used to register it with the
 * {@link org.apache.kafka.streams.processor.internals.InternalTopologyBuilder}
 * <p>
 * Used by the Join nodes as there are several parameters, this abstraction helps keep the number of
 * arguments more reasonable.
 */
public class ProcessorParameters<KIn, VIn, KOut, VOut> {

    private final ProcessorSupplier<KIn, VIn, KOut, VOut> processorSupplier;
    private final String processorName;

    public ProcessorParameters(final ProcessorSupplier<KIn, VIn, KOut, VOut> processorSupplier,
                               final String processorName) {
        this.processorSupplier = processorSupplier;
        this.processorName = processorName;
    }

    public ProcessorSupplier<KIn, VIn, KOut, VOut> processorSupplier() {
        return processorSupplier;
    }

    @SuppressWarnings("unchecked")
    KTableSource<KIn, VIn> kTableSourceSupplier() {
        // This cast always works because KTableSource hasn't been converted yet.
        return processorSupplier == null
            ? null
            : !(processorSupplier instanceof KTableSource)
                ? null
                : (KTableSource<KIn, VIn>) processorSupplier;
    }

    @SuppressWarnings("unchecked")
    <VR> KTableChangeProcessorSupplier<KIn, VIn, VR, KIn, VR> kTableChangeProcessorSupplier() {
        // This cast always works because KTableProcessorSupplier hasn't been converted yet.
        return (KTableChangeProcessorSupplier<KIn, VIn, VR, KIn, VR>) processorSupplier;
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
