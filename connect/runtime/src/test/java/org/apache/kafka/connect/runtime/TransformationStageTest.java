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
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class TransformationStageTest {

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

        @SuppressWarnings("unchecked")
        Predicate<SourceRecord> predicate = mock(Predicate.class);
        when(predicate.test(any())).thenReturn(predicateResult);
        @SuppressWarnings("unchecked")
        Transformation<SourceRecord> transformation = mock(Transformation.class);
        when(transformation.apply(any())).thenReturn(transformed);
        TransformationStage<SourceRecord> stage = new TransformationStage<>(
                predicate,
                negate,
                transformation);

        assertEquals(expectedResult, stage.apply(initial));

        stage.close();
        verify(predicate).close();
        verify(transformation).close();
    }
}
