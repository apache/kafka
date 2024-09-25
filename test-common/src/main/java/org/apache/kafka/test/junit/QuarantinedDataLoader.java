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
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuarantinedDataLoader {

    private static final QuarantinedTestSelector DEFAULT = (className, methodName) -> Optional.empty();

    private static final Logger log = LoggerFactory.getLogger(QuarantinedDataLoader.class);

    public static QuarantinedTestSelector loadTestCatalog() {
        String name = System.getProperty("kafka.test.catalog.file");
        if (name == null || name.isEmpty()) {
            log.debug("No test catalog loaded, will not quarantine any recently added tests.");
            return DEFAULT;
        }
        Path path = Paths.get(name);
        log.debug("Loading test catalog file {}.", path);

        if (!Files.exists(path)) {
            log.error("Test catalog file {} does not exist.", path);
            return DEFAULT;
        }

        Set<TestAndMethod> allTests = new HashSet<>();
        try (BufferedReader reader = Files.newBufferedReader(path, Charset.defaultCharset())) {
            String line = reader.readLine();
            while (line != null) {
                String[] toks = line.split("#", 2);
                allTests.add(new TestAndMethod(toks[0], toks[1]));
                line = reader.readLine();
            }
        } catch (IOException e) {
            log.error("Error while reading test catalog file.", e);
            return DEFAULT;
        }

        log.debug("Loaded {} test methods from test catalog file {}.", allTests.size(), path);
        return (className, methodName) -> {
            if (allTests.contains(new TestAndMethod(className, methodName))) {
                return Optional.empty();
            } else {
                return Optional.of("new test");
            }
        };
    }
}
