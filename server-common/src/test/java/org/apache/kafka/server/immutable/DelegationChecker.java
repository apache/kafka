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

package org.apache.kafka.server.immutable;

import org.mockito.Mockito;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Facilitate testing of wrapper class delegation.
 *
 * We require the following things to test delegation:
 *
 * 1. A mock object to which the wrapper is expected to delegate method invocations
 * 2. A way to define how the mock is expected to behave when its method is invoked
 * 3. A way to define how to invoke the method on the wrapper
 * 4. A way to test that the method on the mock is invoked correctly when the wrapper method is invoked
 * 5. A way to test that any return value from the wrapper method is correct

 * @param <D> delegate type
 * @param <W> wrapper type
 * @param <T> delegating method return type, if any
 */
public abstract class DelegationChecker<D, W, T> {
    private final D mock;
    private final W wrapper;
    private Consumer<D> mockConsumer;
    private Function<D, T> mockConfigurationFunction;
    private T mockFunctionReturnValue;
    private Consumer<W> wrapperConsumer;
    private Function<W, T> wrapperFunctionApplier;
    private Function<T, ?> mockFunctionReturnValueTransformation;
    private boolean expectWrapperToWrapMockFunctionReturnValue;
    private boolean persistentCollectionMethodInvokedCorrectly = false;

    /**
     * @param mock mock for the underlying delegate
     * @param wrapperCreator how to create a wrapper for the mock
     */
    protected DelegationChecker(D mock, Function<D, W> wrapperCreator) {
        this.mock = Objects.requireNonNull(mock);
        this.wrapper = Objects.requireNonNull(wrapperCreator).apply(mock);
    }

    /**
     * @param wrapper the wrapper
     * @return the underlying delegate for the given wrapper
     */
    public abstract D unwrap(W wrapper);

    public DelegationChecker<D, W, T> defineMockConfigurationForVoidMethodInvocation(Consumer<D> mockConsumer) {
        this.mockConsumer = Objects.requireNonNull(mockConsumer);
        return this;
    }

    public DelegationChecker<D, W, T> defineMockConfigurationForFunctionInvocation(Function<D, T> mockConfigurationFunction, T mockFunctionReturnValue) {
        this.mockConfigurationFunction = Objects.requireNonNull(mockConfigurationFunction);
        this.mockFunctionReturnValue = mockFunctionReturnValue;
        return this;
    }

    public DelegationChecker<D, W, T> defineMockConfigurationForUnsupportedFunction(Function<D, T> mockConfigurationFunction) {
        this.mockConfigurationFunction = Objects.requireNonNull(mockConfigurationFunction);
        return this;
    }

    public DelegationChecker<D, W, T> defineWrapperVoidMethodInvocation(Consumer<W> wrapperConsumer) {
        this.wrapperConsumer = Objects.requireNonNull(wrapperConsumer);
        return this;
    }

    public <R> DelegationChecker<D, W, T> defineWrapperFunctionInvocationAndMockReturnValueTransformation(
        Function<W, T> wrapperFunctionApplier,
        Function<T, R> expectedFunctionReturnValueTransformation) {
        this.wrapperFunctionApplier = Objects.requireNonNull(wrapperFunctionApplier);
        this.mockFunctionReturnValueTransformation = Objects.requireNonNull(expectedFunctionReturnValueTransformation);
        return this;
    }

    public DelegationChecker<D, W, T> defineWrapperUnsupportedFunctionInvocation(
            Function<W, T> wrapperFunctionApplier) {
        this.wrapperFunctionApplier = Objects.requireNonNull(wrapperFunctionApplier);
        return this;
    }

    public DelegationChecker<D, W, T> expectWrapperToWrapMockFunctionReturnValue() {
        this.expectWrapperToWrapMockFunctionReturnValue = true;
        return this;
    }

    public void doVoidMethodDelegationCheck() {
        if (mockConsumer == null || wrapperConsumer == null ||
            mockConfigurationFunction != null || wrapperFunctionApplier != null ||
            mockFunctionReturnValue != null || mockFunctionReturnValueTransformation != null) {
            throwExceptionForIllegalTestSetup();
        }
        // configure the mock to behave as desired
        mockConsumer.accept(Mockito.doAnswer(invocation -> {
            persistentCollectionMethodInvokedCorrectly = true;
            return null;
        }).when(mock));
        // invoke the wrapper, which should invoke the mock as desired
        wrapperConsumer.accept(wrapper);
        // assert that the expected delegation to the mock actually occurred
        assertTrue(persistentCollectionMethodInvokedCorrectly);
    }

    public void doUnsupportedVoidFunctionDelegationCheck() {
        if (mockConsumer == null || wrapperConsumer == null) {
            throwExceptionForIllegalTestSetup();
        }

        // configure the mock to behave as desired
        mockConsumer.accept(Mockito.doCallRealMethod().when(mock));

        assertThrows(UnsupportedOperationException.class, () -> wrapperConsumer.accept(wrapper),
            "Expected to Throw UnsupportedOperationException");
    }

    @SuppressWarnings("unchecked")
    public void doFunctionDelegationCheck() {
        if (mockConfigurationFunction == null || wrapperFunctionApplier == null ||
            mockFunctionReturnValueTransformation == null ||
            mockConsumer != null || wrapperConsumer != null) {
            throwExceptionForIllegalTestSetup();
        }
        // configure the mock to behave as desired
        when(mockConfigurationFunction.apply(mock)).thenAnswer(invocation -> {
            persistentCollectionMethodInvokedCorrectly = true;
            return mockFunctionReturnValue;
        });
        // invoke the wrapper, which should invoke the mock as desired
        T wrapperReturnValue = wrapperFunctionApplier.apply(wrapper);
        // assert that the expected delegation to the mock actually occurred, including any return value transformation
        assertTrue(persistentCollectionMethodInvokedCorrectly);
        Object transformedMockFunctionReturnValue = mockFunctionReturnValueTransformation.apply(mockFunctionReturnValue);
        if (this.expectWrapperToWrapMockFunctionReturnValue) {
            assertEquals(transformedMockFunctionReturnValue, unwrap((W) wrapperReturnValue));
        } else {
            assertEquals(transformedMockFunctionReturnValue, wrapperReturnValue);
        }
    }

    public void doUnsupportedFunctionDelegationCheck() {
        if (mockConfigurationFunction == null || wrapperFunctionApplier == null) {
            throwExceptionForIllegalTestSetup();
        }

        when(mockConfigurationFunction.apply(mock)).thenCallRealMethod();
        assertThrows(UnsupportedOperationException.class, () -> wrapperFunctionApplier.apply(wrapper),
            "Expected to Throw UnsupportedOperationException");
    }

    private static void throwExceptionForIllegalTestSetup() {
        throw new IllegalStateException(
            "test setup error: must define both mock and wrapper consumers or both mock and wrapper functions");
    }
}
