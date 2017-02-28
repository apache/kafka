/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.ProcessorContext;

class KTableKTableLeftJoinValueGetter<K1, K2, V1, V2, R> implements KTableValueGetter<K1, R> {

    private final KTableValueGetter<K1, V1> valueGetter1;
    private final KTableValueGetter<K2, V2> valueGetter2;
    private final ValueJoiner<V1, V2, R> joiner;
    private final KeyValueMapper<K1, V1, K2> keyMapper;

    KTableKTableLeftJoinValueGetter(final KTableValueGetter<K1, V1> valueGetter1,
                                    final KTableValueGetter<K2, V2> valueGetter2,
                                    final ValueJoiner<V1, V2, R> joiner,
                                    final KeyValueMapper<K1, V1, K2> keyMapper) {
        this.valueGetter1 = valueGetter1;
        this.valueGetter2 = valueGetter2;
        this.joiner = joiner;
        this.keyMapper = keyMapper;
    }

    @Override
    public void init(ProcessorContext context) {
        valueGetter1.init(context);
        valueGetter2.init(context);
    }

    @Override
    public R get(K1 key) {
        V1 value1 = valueGetter1.get(key);

        if (value1 != null) {
            V2 value2 = valueGetter2.get(keyMapper.apply(key, value1));
            return joiner.apply(value1, value2);
        } else {
            return null;
        }
    }

}
