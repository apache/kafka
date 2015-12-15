/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.streams.kstream.AggregateSupplier;
import org.apache.kafka.streams.kstream.Aggregator;

public class SumSupplier<K, V> implements AggregateSupplier<K, V, V> {

    private class LongSum<K> implements Aggregator<K, Long, Long> {

        @Override
        public Long initialValue(){
            return 0L;
        }

        @Override
        public Long add(K aggKey, Long value, Long aggregate) {
            return aggregate + value;
        }

        @Override
        public Long remove(K aggKey, Long value, Long aggregate) {
            return aggregate - value;
        }

        @Override
        public Long merge(Long aggr1, Long aggr2) {
            return aggr1 + aggr2;
        }
    }

    private class IntSum<K> implements Aggregator<K, Integer, Integer> {

        @Override
        public Integer initialValue(){
            return 0;
        }

        @Override
        public Integer add(K aggKey, Integer value, Integer aggregate) {
            return aggregate + value;
        }

        @Override
        public Integer remove(K aggKey, Integer value, Integer aggregate) {
            return aggregate - value;
        }

        @Override
        public Integer merge(Integer aggr1, Integer aggr2) {
            return aggr1 + aggr2;
        }
    }

    private class ShortSum<K> implements Aggregator<K, Short, Short> {

        @Override
        public Short initialValue(){
            return (short) 0;
        }

        @Override
        public Short add(K aggKey, Short value, Short aggregate) {
            return (short) (aggregate + value);
        }

        @Override
        public Short remove(K aggKey, Short value, Short aggregate) {
            return (short) (aggregate - value);
        }

        @Override
        public Short merge(Short aggr1, Short aggr2) {
            return (short) (aggr1 + aggr2);
        }
    }

    private class DoubleSum<K> implements Aggregator<K, Double, Double> {

        @Override
        public Double initialValue(){
            return 0.0d;
        }

        @Override
        public Double add(K aggKey, Double value, Double aggregate) {
            return aggregate + value;
        }

        @Override
        public Double remove(K aggKey, Double value, Double aggregate) {
            return aggregate - value;
        }

        @Override
        public Double merge(Double aggr1, Double aggr2) {
            return aggr1 + aggr2;
        }
    }

    private class FloatSum<K> implements Aggregator<K, Float, Float> {

        @Override
        public Float initialValue(){
            return 0.0f;
        }

        @Override
        public Float add(K aggKey, Float value, Float aggregate) {
            return aggregate + value;
        }

        @Override
        public Float remove(K aggKey, Float value, Float aggregate) {
            return aggregate - value;
        }

        @Override
        public Float merge(Float aggr1, Float aggr2) {
            return aggr1 + aggr2;
        }
    }

    private class ByteSum<K> implements Aggregator<K, Byte, Byte> {

        @Override
        public Byte initialValue(){
            return (byte) 0L;
        }

        @Override
        public Byte add(K aggKey, Byte value, Byte aggregate) {
            long aggValue = aggregate;

            return (byte) (aggValue + value);
        }

        @Override
        public Byte remove(K aggKey, Byte value, Byte aggregate) {
            long aggValue = aggregate;

            return (byte) (aggValue - value);
        }

        @Override
        public Byte merge(Byte aggr1, Byte aggr2) {
            long aggValue1 = aggr1;

            return (byte) (aggValue1 + aggr2);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Aggregator<K, V, V> get(Class<K> keyClass, Class<V> valueClass, Class<V> aggClass) {
        if (valueClass == Long.class) {
            return (Aggregator<K, V, V>) new LongSum<K>();
        } else if (valueClass == Integer.class) {
            return (Aggregator<K, V, V>) new IntSum<K>();
        } else if (valueClass == Short.class) {
            return (Aggregator<K, V, V>) new ShortSum<K>();
        } else if (valueClass == Double.class) {
            return (Aggregator<K, V, V>) new DoubleSum<K>();
        } else if (valueClass == Float.class) {
            return (Aggregator<K, V, V>) new FloatSum<K>();
        } else if (valueClass == Byte.class) {
            return (Aggregator<K, V, V>) new ByteSum<K>();
        } else {
            throw new KafkaException("The value type " + valueClass.getName() +
                    " is not supported for built-in sum aggregations.");
        }
    }
}