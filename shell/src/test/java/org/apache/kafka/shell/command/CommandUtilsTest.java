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

package org.apache.kafka.shell.command;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(value = 5, unit = MINUTES)
public class CommandUtilsTest {
    @Test
    public void testSplitPath() {
        assertEquals(List.of("alpha", "beta"),
            CommandUtils.splitPath("/alpha/beta"));
        assertEquals(List.of("alpha", "beta"),
            CommandUtils.splitPath("//alpha/beta/"));
    }

    @Test
    public void testStripDotPathComponents() {

        //double dots
        assertEquals(List.of("keep", "keep2"), CommandUtils.stripDotPathComponents(List.of("..", "keep", "keep2")));
        //single dots
        assertEquals(List.of("keep", "keep2"), CommandUtils.stripDotPathComponents(List.of(".", "keep", "keep2")));

        assertEquals(List.of(".keep", "keep2"), CommandUtils.stripDotPathComponents(List.of(".", ".keep", "keep2")));

        assertEquals(List.of(".keep", "keep2"), CommandUtils.stripDotPathComponents(List.of("..", ".keep", "keep2")));

    }
}
