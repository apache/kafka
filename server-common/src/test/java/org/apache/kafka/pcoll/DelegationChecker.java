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

import org.mockito.Mockito;
import org.mockito.stubbing.OngoingStubbing;
import org.mockito.stubbing.Stubber;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// class to facilitate testing of wrapper class delegation to underlying persistent collection
@SuppressWarnings("rawtypes")
public class DelegationChecker<T> {
    private T answerFromMockPersistentCollection;
    private boolean persistentCollectionMethodInvokedCorrectly = false;

    /**
     * Constructor to use when testing delegation of a void method
     */
    public DelegationChecker() {
    }

    public DelegationChecker(T answerFromMockPersistentCollection) {
        this();
        this.answerFromMockPersistentCollection = answerFromMockPersistentCollection;
    }

    public DelegationChecker setAnswerFromMockPersistentCollection(T answerFromMockPersistentCollection) {
        this.answerFromMockPersistentCollection = answerFromMockPersistentCollection;
        return this;
    }

    public <R> DelegationChecker<T> recordsInvocationAndAnswers(OngoingStubbing<R> whenMockedMethodInvoked) {
        whenMockedMethodInvoked.thenAnswer(invocation -> {
            persistentCollectionMethodInvokedCorrectly = true;
            return answerFromMockPersistentCollection;
        });
        return this;
    }

    public void assertDelegatedCorrectly() {
        assertTrue(persistentCollectionMethodInvokedCorrectly);
    }

    public void assertDelegatesAndAnswersCorrectly(Object receivedAnswer) {
        assertDelegatedCorrectly();
        assertEquals(answerFromMockPersistentCollection, receivedAnswer);
    }

    public Stubber recordVoidMethodInvocation() {
        return Mockito.doAnswer(invocation -> {
            persistentCollectionMethodInvokedCorrectly = true;
            return null;
        });
    }
}
