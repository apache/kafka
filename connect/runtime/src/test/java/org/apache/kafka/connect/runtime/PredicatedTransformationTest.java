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

import org.apache.kafka.connect.source.SourceRecord;
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

        SamplePredicate predicate = new SamplePredicate(predicateResult);
        SampleTransformation<SourceRecord> predicatedTransform = new SampleTransformation<>(transformed);
        PredicatedTransformation<SourceRecord> pt = new PredicatedTransformation<>(
                predicate,
                negate,
                predicatedTransform);

        assertEquals(expectedResult, pt.apply(initial));

        pt.close();
        assertTrue(predicate.closed);
        assertTrue(predicatedTransform.closed);
    }
}
