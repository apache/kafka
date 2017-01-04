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
package org.apache.kafka.connect.util;

/**
 * An UncaughtExceptionHandler that can be registered with one or more threads which tracks the
 * first exception so the main thread can check for uncaught exceptions.
 */
public class TestBackgroundThreadExceptionHandler implements Thread.UncaughtExceptionHandler {
    private Throwable firstException = null;

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        if (this.firstException == null)
            this.firstException = e;
    }

    public void verifyNoExceptions() {
        if (this.firstException != null)
            throw new AssertionError(this.firstException);
    }
}
