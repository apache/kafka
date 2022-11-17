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
package org.apache.kafka.connect.test.util;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.verification.VerificationMode;

import java.util.Arrays;

import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mockingDetails;

public class MockitoUtils {

    /**
     * Create a verification mode that accepts any number of method invocations, including zero.
     * <p>
     * Sample usage:
     * <p>
     * {@code verify(sourceTask, anyTimes()).poll();}
     * @return the verification mode; never null
     */
    public static VerificationMode anyTimes() {
        return atLeast(0);
    }

    /**
     * Count the number of times a method has been invoked on a mock object.
     * <p>
     * Sample usage:
     * <p>
     * <pre>
     * Producer<byte[], byte[]> producer = mock(Producer.class);
     * // ... run through some test case that uses the mocked producer
     * assertEquals(
     *     "Producer should have aborted every record it sent",
     *     countInvocations(producer, "abortTransaction"),
     *     countInvocations(producer, "send", ProducerRecord.class, Callback.class)
     * );
     * </pre>
     * @param mock the mock object whose method invocations should be counted; may not be null
     * @param methodName the name of the method whose invocations should be counted; may not be null
     * @param parameters the types of the parameters for the method whose invocations should be counted;
     *                   may be empty, but may not contain any null elements
     * @return the number of times the method was invoked on the mock
     */
    public static long countInvocations(Object mock, String methodName, Class<?>... parameters) {
        return mockingDetails(mock).getInvocations().stream()
                .map(InvocationOnMock::getMethod)
                .filter(m -> methodName.equals(m.getName()))
                .filter(m -> Arrays.equals(parameters, m.getParameterTypes()))
                .count();
    }

}
