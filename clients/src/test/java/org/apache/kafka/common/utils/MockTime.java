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
package org.apache.kafka.common.utils;

import java.util.concurrent.TimeUnit;

/**
 * A clock that you can manually advance by calling sleep
 */
public class MockTime implements Time {

    private long nanos = 0;
    private long autoTickMs = 0;

    public MockTime() {
        this.nanos = System.nanoTime();
    }

    public MockTime(long autoTickMs) {
        this.nanos = System.nanoTime();
        this.autoTickMs = autoTickMs;
    }

    @Override
    public long milliseconds() {
        this.sleep(autoTickMs);
        return TimeUnit.MILLISECONDS.convert(this.nanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public long nanoseconds() {
        this.sleep(autoTickMs);
        return nanos;
    }

    @Override
    public void sleep(long ms) {
        this.nanos += TimeUnit.NANOSECONDS.convert(ms, TimeUnit.MILLISECONDS);
    }

}
