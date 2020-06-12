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
package org.apache.kafka.connect.runtime;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.junit.Test;

import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PredicatedTransformationTest {

    private final SourceRecord initial = new SourceRecord(singletonMap("initial", 1), null, null, null, null);
    private final SourceRecord transformed = new SourceRecord(singletonMap("transformed", 2), null, null, null, null);

    @Test
    public void apply() {
        applyAndAssert(true, false, transformed);
        applyAndAssert(true, true, initial);
        applyAndAssert(false, false, initial);
        applyAndAssert(false, true, transformed);
    }

    private void applyAndAssert(boolean predicateResult, boolean negate,
                                SourceRecord expectedResult) {
        class TestTransformation implements Transformation<SourceRecord> {

            private boolean closed = false;
            private SourceRecord transformedRecord;

            private TestTransformation(SourceRecord transformedRecord) {
                this.transformedRecord = transformedRecord;
            }

            @Override
            public SourceRecord apply(SourceRecord record) {
                return transformedRecord;
            }

            @Override
            public ConfigDef config() {
                return null;
            }

            @Override
            public void close() {
                closed = true;
            }

            @Override
            public void configure(Map<String, ?> configs) {

            }

            private void assertClosed() {
                assertTrue("Transformer should be closed", closed);
            }
        }

        class TestPredicate implements Predicate<SourceRecord> {

            private boolean testResult;
            private boolean closed = false;

            private TestPredicate(boolean testResult) {
                this.testResult = testResult;
            }

            @Override
            public ConfigDef config() {
                return null;
            }

            @Override
            public boolean test(SourceRecord record) {
                return testResult;
            }

            @Override
            public void close() {
                closed = true;
            }

            @Override
            public void configure(Map<String, ?> configs) {

            }

            private void assertClosed() {
                assertTrue("Predicate should be closed", closed);
            }
        }
        TestPredicate predicate = new TestPredicate(predicateResult);
        TestTransformation predicatedTransform = new TestTransformation(transformed);
        PredicatedTransformation<SourceRecord> pt = new PredicatedTransformation<>(
                predicate,
                negate,
                predicatedTransform);

        assertEquals(expectedResult, pt.apply(initial));

        pt.close();
        predicate.assertClosed();
        predicatedTransform.assertClosed();
    }
}
