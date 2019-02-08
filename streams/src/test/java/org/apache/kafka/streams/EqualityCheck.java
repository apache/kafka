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
package org.apache.kafka.streams;

public final class EqualityCheck {
    private EqualityCheck() {}

    // Inspired by EqualsTester from Guava
    public static <T> void verifyEquality(final T o1, final T o2) {
        // making sure we don't get an NPE in the test
        if (o1 == null && o2 == null) {
            return;
        } else if (o1 == null) {
            throw new AssertionError(String.format("o1 was null, but o2[%s] was not.", o2));
        } else if (o2 == null) {
            throw new AssertionError(String.format("o1[%s] was not null, but o2 was.", o1));
        }
        verifyGeneralEqualityProperties(o1, o2);


        // the two objects should equal each other
        if (!o1.equals(o2)) {
            throw new AssertionError(String.format("o1[%s] was not equal to o2[%s].", o1, o2));
        }

        if (!o2.equals(o1)) {
            throw new AssertionError(String.format("o2[%s] was not equal to o1[%s].", o2, o1));
        }

        verifyHashCodeConsistency(o1, o2);

        // since these objects are equal, their hashcode MUST be the same
        if (o1.hashCode() != o2.hashCode()) {
            throw new AssertionError(String.format("o1[%s].hash[%d] was not equal to o2[%s].hash[%d].", o1, o1.hashCode(), o2, o2.hashCode()));
        }
    }

    public static <T> void verifyInEquality(final T o1, final T o2) {
        // making sure we don't get an NPE in the test
        if (o1 == null && o2 == null) {
            throw new AssertionError("Both o1 and o2 were null.");
        } else if (o1 == null) {
            return;
        } else if (o2 == null) {
            return;
        }

        verifyGeneralEqualityProperties(o1, o2);

        // these two objects should NOT equal each other
        if (o1.equals(o2)) {
            throw new AssertionError(String.format("o1[%s] was equal to o2[%s].", o1, o2));
        }

        if (o2.equals(o1)) {
            throw new AssertionError(String.format("o2[%s] was equal to o1[%s].", o2, o1));
        }
        verifyHashCodeConsistency(o1, o2);


        // since these objects are NOT equal, their hashcode SHOULD PROBABLY not be the same
        if (o1.hashCode() == o2.hashCode()) {
            throw new AssertionError(
                String.format(
                    "o1[%s].hash[%d] was equal to o2[%s].hash[%d], even though !o1.equals(o2). " +
                        "This is NOT A BUG, but it is undesirable for hash collection performance.",
                    o1,
                    o1.hashCode(),
                    o2,
                    o2.hashCode()
                )
            );
        }
    }


    @SuppressWarnings({"EqualsWithItself", "ConstantConditions", "ObjectEqualsNull"})
    private static <T> void verifyGeneralEqualityProperties(final T o1, final T o2) {
        // objects should equal themselves
        if (!o1.equals(o1)) {
            throw new AssertionError(String.format("o1[%s] was not equal to itself.", o1));
        }

        if (!o2.equals(o2)) {
            throw new AssertionError(String.format("o2[%s] was not equal to itself.", o2));
        }

        // non-null objects should not equal null
        if (o1.equals(null)) {
            throw new AssertionError(String.format("o1[%s] was equal to null.", o1));
        }

        if (o2.equals(null)) {
            throw new AssertionError(String.format("o2[%s] was equal to null.", o2));
        }

        // objects should not equal some random object
        if (o1.equals(new Object())) {
            throw new AssertionError(String.format("o1[%s] was equal to an anonymous Object.", o1));
        }

        if (o2.equals(new Object())) {
            throw new AssertionError(String.format("o2[%s] was equal to an anonymous Object.", o2));
        }
    }


    private static <T> void verifyHashCodeConsistency(final T o1, final T o2) {
        {
            final int first = o1.hashCode();
            final int second = o1.hashCode();
            if (first != second) {
                throw new AssertionError(
                    String.format(
                        "o1[%s]'s hashcode was not consistent: [%d]!=[%d].",
                        o1,
                        first,
                        second
                    )
                );
            }
        }

        {
            final int first = o2.hashCode();
            final int second = o2.hashCode();
            if (first != second) {
                throw new AssertionError(
                    String.format(
                        "o2[%s]'s hashcode was not consistent: [%d]!=[%d].",
                        o2,
                        first,
                        second
                    )
                );
            }
        }
    }
}
