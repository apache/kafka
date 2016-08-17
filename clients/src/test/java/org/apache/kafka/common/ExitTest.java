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

import org.apache.kafka.common.errors.FatalExitException;
import org.apache.kafka.common.utils.Utils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FatalExitException.class})
public class ExitTest {
    @Before
    public void init() {
        // TODO: to be replaced with proper mocking
        FatalExitException.testMode();
    }

    @Test
    public void testSystemExit() {
        Thread t = Utils.newThread("test-exit-thread", new Runnable() {
            @Override
            public void run() {
            }
        }, false);
        t.getUncaughtExceptionHandler().uncaughtException(t, new FatalExitException(1));
        /** if we reach this point it means that the {@link FatalExitException} is properly caught
         and yet it successfully disabled invoking {@link System.exit} for unit tests
         */
        assertTrue(true);
    }
}
