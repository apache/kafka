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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class QuarantinedDataLoader {

    private static final QuarantinedTestSelector DEFAULT = (className, methodName) -> Optional.empty();

    public static QuarantinedTestSelector loadQuarantinedTests() {
        return DEFAULT;
    }

    public static QuarantinedTestSelector loadMainTests() {
        try {
            String name = System.getProperty("kafka.test.suites.main.list");
            File file = new File(name);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line = br.readLine();
            Set<TestAndMethod> allTests = new HashSet<>();
            while (line != null) {
                String[] toks = line.split("#");
                allTests.add(new TestAndMethod(toks[0], toks[1]));
                line = br.readLine();
            }
            return (className, methodName) -> {
                if (allTests.contains(new TestAndMethod(className, methodName))) {
                    return Optional.empty();
                } else {
                    return Optional.of("new test");
                }
            };
        } catch (Exception e) {
            return DEFAULT;
        }
    }
}
