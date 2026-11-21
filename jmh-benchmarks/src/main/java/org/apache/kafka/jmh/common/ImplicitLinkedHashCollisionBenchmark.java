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

package org.apache.kafka.jmh.common;

import org.apache.kafka.common.utils.internals.ImplicitLinkedHashCollection;
import org.apache.kafka.common.utils.internals.ImplicitLinkedHashMultiCollection;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ImplicitLinkedHashCollisionBenchmark {

    @Param({"1000", "10000", "100000"})
    private int size;

    static final class BenchElement implements ImplicitLinkedHashCollection.Element {
        private int prev = ImplicitLinkedHashCollection.INVALID_INDEX;
        private int next = ImplicitLinkedHashCollection.INVALID_INDEX;
        private final int key;
        private final int hash;

        BenchElement(int key, int hash) {
            this.key = key;
            this.hash = hash;
        }

        @Override
        public int prev() {
            return prev;
        }

        @Override
        public void setPrev(int prev) {
            this.prev = prev;
        }

        @Override
        public int next() {
            return next;
        }

        @Override
        public void setNext(int next) {
            this.next = next;
        }

        @Override
        public boolean elementKeysAreEqual(Object o) {
            if (this == o) return true;
            if ((o == null) || (o.getClass() != BenchElement.class)) return false;
            return key == ((BenchElement) o).key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if ((o == null) || (o.getClass() != BenchElement.class)) return false;
            BenchElement that = (BenchElement) o;
            return key == that.key && hash == that.hash;
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    private static int scramble(int key) {
        return (int) ((2654435761L * key) >> 16);
    }

    @Benchmark
    public Object buildMultiCollectionIdenticalKeys() {
        ImplicitLinkedHashMultiCollection<BenchElement> coll = new ImplicitLinkedHashMultiCollection<>(size);
        try {
            for (int i = 0; i < size; i++) {
                coll.add(new BenchElement(1, 0));
            }
        } catch (RuntimeException e) {
            return e;
        }
        return coll;
    }

    @Benchmark
    public Object buildCollectionDistinctKeys() {
        ImplicitLinkedHashCollection<BenchElement> coll = new ImplicitLinkedHashCollection<>(size);
        for (int i = 0; i < size; i++) {
            coll.add(new BenchElement(i, scramble(i)));
        }
        return coll;
    }
}
