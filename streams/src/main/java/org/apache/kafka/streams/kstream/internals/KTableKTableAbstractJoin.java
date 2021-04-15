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

import org.apache.kafka.streams.kstream.ValueJoiner;

abstract class KTableKTableAbstractJoin<K, V, V1, VOut> implements KTableChangeProcessorSupplier<K, V, VOut, K, VOut> {

    private final KTableImpl<K, ?, V> table;
    private final KTableImpl<K, ?, V1> other;
    final KTableValueGetterSupplier<K, V> valueGetterSupplier1;
    final KTableValueGetterSupplier<K, V1> valueGetterSupplier2;
    final ValueJoiner<? super V, ? super V1, ? extends VOut> joiner;

    boolean sendOldValues = false;

    KTableKTableAbstractJoin(final KTableImpl<K, ?, V> table,
                             final KTableImpl<K, ?, V1> other,
                             final ValueJoiner<? super V, ? super V1, ? extends VOut> joiner) {
        this.table = table;
        this.other = other;
        this.valueGetterSupplier1 = table.valueGetterSupplier();
        this.valueGetterSupplier2 = other.valueGetterSupplier();
        this.joiner = joiner;
    }

    @Override
    public final boolean enableSendingOldValues(final boolean forceMaterialization) {
        // Table-table joins require upstream materialization:
        table.enableSendingOldValues(true);
        other.enableSendingOldValues(true);
        sendOldValues = true;
        return true;
    }
}
