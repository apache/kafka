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

package org.apache.kafka.pcoll;

import org.mockito.stubbing.OngoingStubbing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// class to facilitate testing of wrapper class delegation to underlying persistent collection
@SuppressWarnings("rawtypes")
public class DelegationChecker<T> {
    private final T answerFromMockPersistentCollection;
    private boolean persistentCollectionMethodInvokedCorrectly = false;

    public DelegationChecker(T answerFromMockPersistentCollection) {
        this.answerFromMockPersistentCollection = answerFromMockPersistentCollection;
    }

    public <R> DelegationChecker<T> answers(OngoingStubbing<R> whenMockedMethodInvoked) {
        whenMockedMethodInvoked.thenAnswer(invocation -> {
            persistentCollectionMethodInvokedCorrectly = true;
            return answerFromMockPersistentCollection;
        });
        return this;
    }

    public void assertDelegatesAndAnswersCorrectly(Object receivedAnswer) {
        assertTrue(persistentCollectionMethodInvokedCorrectly);
        assertEquals(answerFromMockPersistentCollection, receivedAnswer);
    }
}
