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

abstract class KTableKTableAbstractJoin<K, R, V1, V2> implements KTableProcessorSupplier<K, V1, R> {

    private final KTableImpl<K, ?, V1> table1;
    private final KTableImpl<K, ?, V2> table2;
    final KTableValueGetterSupplier<K, V1> valueGetterSupplier1;
    final KTableValueGetterSupplier<K, V2> valueGetterSupplier2;
    final ValueJoiner<? super V1, ? super V2, ? extends R> joiner;

    boolean sendOldValues = false;

    KTableKTableAbstractJoin(final KTableImpl<K, ?, V1> table1,
                             final KTableImpl<K, ?, V2> table2,
                             final ValueJoiner<? super V1, ? super V2, ? extends R> joiner) {
        this.table1 = table1;
        this.table2 = table2;
        this.valueGetterSupplier1 = table1.valueGetterSupplier();
        this.valueGetterSupplier2 = table2.valueGetterSupplier();
        this.joiner = joiner;
    }

    @Override
    public final void enableSendingOldValues() {
        table1.enableSendingOldValues();
        table2.enableSendingOldValues();
        sendOldValues = true;
    }
}
