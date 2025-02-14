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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;

import java.util.List;

import static java.util.Arrays.asList;

public class ThrottledReplicaListValidator implements Validator {
    public static final Validator INSTANCE = new ThrottledReplicaListValidator();

    private ThrottledReplicaListValidator() { }

    public static void ensureValidString(String name, String value) {
        INSTANCE.ensureValid(name, asList(value.split(",")));
    }

    @Override
    public void ensureValid(String name, Object value) {
        if (value instanceof java.util.List<?>) {
            List<String> proposed = ((List<?>) value).stream().map(element -> element.toString().trim()).toList();
            if (!(proposed.stream().allMatch(s -> s.matches("([0-9]+:[0-9]+)?"))
                    || String.join("", proposed).equals("*")))
                throw new ConfigException(name, value, name +
                    " must be the literal '*' or a list of replicas in the following format: [partitionId]:[brokerId],[partitionId]:[brokerId],...");
        } else
            throw new ConfigException(name, value, name + " must be a List but was " + value.getClass().getName());
    }

    @Override
    public String toString() {
        return "[partitionId]:[brokerId],[partitionId]:[brokerId],...";
    }
}
