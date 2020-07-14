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

import static org.junit.Assert.fail;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestUnitTest {
    final Logger log = LoggerFactory.getLogger(TestUnitTest.class);

    @Test
    public void testTest() {
        log.error("ERROR level log");
        log.warn("WARN level log");
        log.info("INFO level log");
        log.debug("DEBUG level log");
        log.trace("TRACE level log");

        System.out.println("Printing to stdout");
        System.err.println("Printing to stderr");

        fail("This unit test has failed :(");
    }
}
