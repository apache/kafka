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

import org.apache.kafka.streams.kstream.Reducer;

public class MockReducer {

    private static class StringAdd implements Reducer<String> {

        @Override
        public String apply(String value1, String value2) {
            return value1 + "+" + value2;
        }
    }

    private static class StringRemove implements Reducer<String> {

        @Override
        public String apply(String value1, String value2) {
            return value1 + "-" + value2;
        }
    }

    public final static Reducer<String> STRING_ADDER = new StringAdd();

    public final static Reducer<String> STRING_REMOVER = new StringRemove();
}