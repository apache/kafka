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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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

    private static class FatalExitError4Test extends FatalExitError {
        boolean shutdownIsInvoked;
        Thread shutdownInvokerThread;
        FatalExitError4Test(int exitStatus) {
            super(exitStatus);
        }
        @Override
        protected void setUncaughtExceptionHandler(Thread t) {
            shutdownInvokerThread = t;
            t.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    shutdownIsInvoked = e instanceof ExitException4Test;
                }
            });
        }
    };

    @Test
    public void testSystemExitInUtilThreads() {
        Thread t = Utils.newThread("test-exit-thread", new Runnable() {
            @Override
            public void run() {
            }
        }, false);
        FatalExitError4Test exitError = new FatalExitError4Test(1);
        t.getUncaughtExceptionHandler().uncaughtException(t, exitError);
        try {
            exitError.shutdownInvokerThread.join(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
            fail();
        }
        assertTrue(exitError.shutdownIsInvoked);
    }
}

