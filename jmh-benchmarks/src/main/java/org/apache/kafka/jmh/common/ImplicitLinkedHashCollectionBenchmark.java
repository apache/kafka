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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Comparator;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 6)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ImplicitLinkedHashCollectionBenchmark {
    public static class TestElement implements ImplicitLinkedHashCollection.Element {
        private final String value;
        private int next = ImplicitLinkedHashCollection.INVALID_INDEX;
        private int prev = ImplicitLinkedHashCollection.INVALID_INDEX;

        public TestElement(String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }

        @Override
        public int prev() {
            return this.prev;
        }

        @Override
        public void setPrev(int prev) {
            this.prev = prev;
        }

        @Override
        public int next() {
            return this.next;
        }

        @Override
        public void setNext(int next) {
            this.next = next;
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof TestElement)) return false;
            TestElement other = (TestElement) o;
            return value.equals(other.value);
        }
    }

    public static class TestElementComparator implements Comparator<TestElement> {
        public static final TestElementComparator INSTANCE = new TestElementComparator();

        @Override
        public int compare(TestElement a, TestElement b) {
            return a.value().compareTo(b.value());
        }
    }

    @Param({"10000", "100000"})
    private int size;

    private ImplicitLinkedHashCollection<TestElement> coll;

    @Setup(Level.Trial)
    public void setup() {
        coll = new ImplicitLinkedHashCollection<>();
        for (int i = 0; i < size; i++) {
            coll.add(new TestElement(Uuid.randomUuid().toString()));
        }
    }

    /**
     * Test sorting the collection entries.
     */
    @Benchmark
    public ImplicitLinkedHashCollection<TestElement> testCollectionSort() {
        coll.sort(TestElementComparator.INSTANCE);
        return coll;
    }
}
