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

package org.apache.kafka.test.junit;

import java.util.Objects;

public class TestAndMethod {
    private final String testClass;
    private final String testMethod;

    public TestAndMethod(String testClass, String testMethod) {
        this.testClass = testClass;
        this.testMethod = testMethod;
    }

    public String getTestClass() {
        return testClass;
    }

    public String getTestMethod() {
        return testMethod;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestAndMethod that = (TestAndMethod) o;
        return Objects.equals(testClass, that.testClass) && Objects.equals(testMethod, that.testMethod);
    }

    @Override
    public int hashCode() {
        return Objects.hash(testClass, testMethod);
    }

    @Override
    public String toString() {
        return "TestAndMethod{" +
                "testClass='" + testClass + '\'' +
                ", testMethod='" + testMethod + '\'' +
                '}';
    }
}
