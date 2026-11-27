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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * Verifies that all KStream API methods accept both lambda expressions and anonymous classes as
 * parameters. Tests in this class pass if they compile — if the generic bounds were wrong, the
 * Java compiler would reject the lambda forms.
 *
 * See KAFKA-8035.
 */
public class KStreamApiTest {

    private KStream<Integer, Integer> stream;

    @BeforeEach
    public void setUp() {
        final StreamsBuilder builder = new StreamsBuilder();
        stream = builder.stream("source", Consumed.with(Serdes.Integer(), Serdes.Integer()));
    }

    // --- filter ---

    @Test
    public void shouldAcceptLambdaForFilter() {
        stream.filter((key, value) -> key > 0);
    }

    @Test
    public void shouldAcceptAnonymousClassForFilter() {
        stream.filter(new Predicate<Integer, Integer>() {
            @Override
            public boolean test(final Integer key, final Integer value) {
                return key > 0;
            }
        });
    }

    // --- filterNot ---

    @Test
    public void shouldAcceptLambdaForFilterNot() {
        stream.filterNot((key, value) -> key < 0);
    }

    @Test
    public void shouldAcceptAnonymousClassForFilterNot() {
        stream.filterNot(new Predicate<Integer, Integer>() {
            @Override
            public boolean test(final Integer key, final Integer value) {
                return key < 0;
            }
        });
    }

    // --- selectKey ---

    @Test
    public void shouldAcceptLambdaForSelectKey() {
        stream.selectKey((key, value) -> key + value);
    }

    @Test
    public void shouldAcceptAnonymousClassForSelectKey() {
        stream.selectKey(new KeyValueMapper<Integer, Integer, Integer>() {
            @Override
            public Integer apply(final Integer key, final Integer value) {
                return key + value;
            }
        });
    }

    // --- map ---

    @Test
    public void shouldAcceptLambdaForMap() {
        stream.map((key, value) -> KeyValue.pair(key, value.toString()));
    }

    @Test
    public void shouldAcceptAnonymousClassForMap() {
        stream.map(new KeyValueMapper<Integer, Integer, KeyValue<Integer, String>>() {
            @Override
            public KeyValue<Integer, String> apply(final Integer key, final Integer value) {
                return KeyValue.pair(key, value.toString());
            }
        });
    }

    // --- mapValues (ValueMapper) ---

    @Test
    public void shouldAcceptLambdaForMapValues() {
        stream.mapValues(value -> value * 2);
    }

    @Test
    public void shouldAcceptAnonymousClassForMapValues() {
        stream.mapValues(new ValueMapper<Integer, Integer>() {
            @Override
            public Integer apply(final Integer value) {
                return value * 2;
            }
        });
    }

    // --- mapValues (ValueMapperWithKey) ---

    @Test
    public void shouldAcceptLambdaForMapValuesWithKey() {
        stream.mapValues((key, value) -> key + value);
    }

    @Test
    public void shouldAcceptAnonymousClassForMapValuesWithKey() {
        stream.mapValues(new ValueMapperWithKey<Integer, Integer, Integer>() {
            @Override
            public Integer apply(final Integer key, final Integer value) {
                return key + value;
            }
        });
    }

    // --- flatMap ---

    @Test
    public void shouldAcceptLambdaForFlatMap() {
        stream.flatMap((key, value) -> Arrays.asList(
            KeyValue.pair(key, value),
            KeyValue.pair(key, value + 1)
        ));
    }

    @Test
    public void shouldAcceptAnonymousClassForFlatMap() {
        stream.flatMap(new KeyValueMapper<Integer, Integer, Iterable<KeyValue<Integer, Integer>>>() {
            @Override
            public Iterable<KeyValue<Integer, Integer>> apply(final Integer key, final Integer value) {
                return Arrays.asList(KeyValue.pair(key, value), KeyValue.pair(key, value + 1));
            }
        });
    }

    // --- flatMapValues (ValueMapper) ---

    @Test
    public void shouldAcceptLambdaForFlatMapValues() {
        stream.flatMapValues(value -> Arrays.asList(value, value + 1));
    }

    @Test
    public void shouldAcceptAnonymousClassForFlatMapValues() {
        stream.flatMapValues(new ValueMapper<Integer, Iterable<Integer>>() {
            @Override
            public Iterable<Integer> apply(final Integer value) {
                return Arrays.asList(value, value + 1);
            }
        });
    }

    // --- flatMapValues (ValueMapperWithKey) ---

    @Test
    public void shouldAcceptLambdaForFlatMapValuesWithKey() {
        stream.flatMapValues((key, value) -> Arrays.asList(value, key + value));
    }

    @Test
    public void shouldAcceptAnonymousClassForFlatMapValuesWithKey() {
        stream.flatMapValues(new ValueMapperWithKey<Integer, Integer, Iterable<Integer>>() {
            @Override
            public Iterable<Integer> apply(final Integer key, final Integer value) {
                return Arrays.asList(value, key + value);
            }
        });
    }

    // --- foreach ---

    @Test
    public void shouldAcceptLambdaForForeach() {
        stream.foreach((key, value) -> { });
    }

    @Test
    public void shouldAcceptAnonymousClassForForeach() {
        stream.foreach(new ForeachAction<Integer, Integer>() {
            @Override
            public void apply(final Integer key, final Integer value) { }
        });
    }

    // --- peek ---

    @Test
    public void shouldAcceptLambdaForPeek() {
        stream.peek((key, value) -> { });
    }

    @Test
    public void shouldAcceptAnonymousClassForPeek() {
        stream.peek(new ForeachAction<Integer, Integer>() {
            @Override
            public void apply(final Integer key, final Integer value) { }
        });
    }

    // --- merge ---

    @Test
    public void shouldAcceptLambdaForFilterOnMergedStream() {
        final StreamsBuilder builder2 = new StreamsBuilder();
        final KStream<Integer, Integer> other = builder2.stream("other", Consumed.with(Serdes.Integer(), Serdes.Integer()));
        stream.merge(other).filter((key, value) -> key > 0);
    }

    // --- method reference ---

    @Test
    public void shouldAcceptMethodReferenceForFilter() {
        stream.filter(KStreamApiTest::isPositive);
    }

    private static boolean isPositive(final Integer key, final Integer value) {
        return key > 0;
    }

    @Test
    public void shouldAcceptMethodReferenceForFlatMapValues() {
        stream.flatMapValues(KStreamApiTest::toSingletonList);
    }

    private static Iterable<Integer> toSingletonList(final Integer value) {
        return Collections.singletonList(value);
    }
}
