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
package org.apache.kafka.connect.transforms.util;

import org.apache.kafka.common.config.ConfigDef;

import java.util.List;

public class NonBlankStringListValidator implements ConfigDef.Validator {

    @SuppressWarnings("unchecked")
    @Override
    public void ensureValid(String name, Object value) {
        final NonEmptyListValidator nonEmptyListValidator = new NonEmptyListValidator();
        final ConfigDef.NonBlankString nonBlankString = new ConfigDef.NonBlankString();
        nonEmptyListValidator.ensureValid(name, value);
        List<String> lst = (List<String>) value;
        lst.forEach(v -> nonBlankString.ensureValid(name, v));
    }

    @Override
    public String toString() {
        return "non-blank string list";
    }

}
