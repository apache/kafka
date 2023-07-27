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

import org.apache.kafka.streams.processor.api.ProcessorSupplier;

public interface KTableProcessorSupplier<KIn, VIn, KOut, VOut> extends ProcessorSupplier<KIn, Change<VIn>, KOut, Change<VOut>> {

    KTableValueGetterSupplier<KOut, VOut> view();

    /**
     * Potentially enables sending old values.
     * <p>
     * If {@code forceMaterialization} is {@code true}, the method will force the materialization of upstream nodes to
     * enable sending old values.
     * <p>
     * If {@code forceMaterialization} is {@code false}, the method will only enable the sending of old values <i>if</i>
     * an upstream node is already materialized.
     *
     * @param forceMaterialization indicates if an upstream node should be forced to materialize to enable sending old
     *                             values.
     * @return {@code true} if sending old values is enabled, i.e. either because {@code forceMaterialization} was
     * {@code true} or some upstream node is materialized.
     */
    boolean enableSendingOldValues(boolean forceMaterialization);
}
