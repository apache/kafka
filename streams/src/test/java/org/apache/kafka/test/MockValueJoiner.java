/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.test;

import org.apache.kafka.streams.kstream.ValueJoiner;

public class MockValueJoiner {

    public final static ValueJoiner<Object, Object, String> TOSTRING_JOINER = instance("+");

    public static <V1, V2> ValueJoiner<V1, V2, String> instance(final String separator) {
        return new ValueJoiner<V1, V2, String>() {
            @Override
            public String apply(V1 value1, V2 value2) {
                return value1 + separator + value2;
            }
        };
    }
}
