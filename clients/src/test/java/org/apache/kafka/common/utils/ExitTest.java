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
package org.apache.kafka.common.utils;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ExitTest {
    @Test
    public void shouldHaltImmediately() {
        List<Object> list = new ArrayList<>();
        Exit.setHaltProcedure((statusCode, message) -> {
            list.add(statusCode);
            list.add(message);
        });
        try {
            int statusCode = 0;
            String message = "mesaage";
            Exit.halt(statusCode);
            Exit.halt(statusCode, message);
            assertEquals(Arrays.asList(statusCode, null, statusCode, message), list);
        } finally {
            Exit.resetHaltProcedure();
        }
    }

    @Test
    public void shouldExitImmediately() {
        List<Object> list = new ArrayList<>();
        Exit.setExitProcedure((statusCode, message) -> {
            list.add(statusCode);
            list.add(message);
        });
        try {
            int statusCode = 0;
            String message = "mesaage";
            Exit.exit(statusCode);
            Exit.exit(statusCode, message);
            assertEquals(Arrays.asList(statusCode, null, statusCode, message), list);
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @Test
    public void shouldAddShutdownHookImmediately() {
        List<Object> list = new ArrayList<>();
        Exit.setShutdownHookAdder((name, runnable) -> {
            list.add(name);
            list.add(runnable);
        });
        try {
            Runnable runnable = () -> { };
            String name = "name";
            Exit.addShutdownHook(name, runnable);
            assertEquals(Arrays.asList(name, runnable), list);
        } finally {
            Exit.resetShutdownHookAdder();
        }
    }

    @Test
    public void shouldNotInvokeShutdownHookImmediately() {
        List<Object> list = new ArrayList<>();
        Runnable runnable = () -> list.add(this);
        Exit.addShutdownHook("message", runnable);
        assertEquals(0, list.size());
    }
}
