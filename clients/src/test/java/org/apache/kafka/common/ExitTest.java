/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common;

import org.apache.kafka.common.errors.FatalExitError;
import org.apache.kafka.common.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ExitTest {

    //TODO: make all unit tests perform this init (perhaps by inheriting from a base test class)
    @Before
    public void init() {
        System.setSecurityManager(new SecurityManager4Test());
    }

    @After
    public void tearDown() {
        System.setSecurityManager(null);
    }

    @Test
    public void testExitIsMocked() throws Exception {
        try {
            System.exit(1);
        } catch (ExitException4Test e) {
            assertEquals("Exit exitStatus", 1, e.exitStatus);
        }
    }

    @Test(expected = ExitException4Test.class)
    public void testSystemExitInUtilThreads() {
        Thread t = Utils.newThread("test-exit-thread", new Runnable() {
            @Override
            public void run() {
            }
        }, false);
        t.getUncaughtExceptionHandler().uncaughtException(t, new FatalExitError(1));
    }
}

