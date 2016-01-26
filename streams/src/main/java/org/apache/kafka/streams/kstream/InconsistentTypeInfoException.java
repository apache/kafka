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

package org.apache.kafka.streams.kstream;

import org.apache.kafka.streams.errors.TopologyBuilderException;

import java.lang.reflect.Type;

public class InconsistentTypeInfoException extends TopologyBuilderException {

    private static final long serialVersionUID = 1L;

    public InconsistentTypeInfoException(Type... types) {
        super(toString(types));
    }

    public InconsistentTypeInfoException(String msg) {
        super("inconsistent type information: " + msg);
    }

    private static String toString(Type... types) {
        StringBuilder sb = new StringBuilder();

        for (Type type : types) {
            sb.append(type.toString());
        }

        return sb.toString();
    }
}
