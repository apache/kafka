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

import org.apache.kafka.common.utils.Time;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A clock that you can manually advance by calling sleep
 */
public class MockTime implements Time {

    private final AtomicLong nanos;

    public MockTime() {
        this.nanos = new AtomicLong(System.nanoTime());
    }

    @Override
    public long milliseconds() {
        return TimeUnit.MILLISECONDS.convert(this.nanos.get(), TimeUnit.NANOSECONDS);
    }

    @Override
    public long hiResClockMs() {
        return TimeUnit.NANOSECONDS.toMillis(nanos.get());
    }

    @Override
    public long nanoseconds() {
        return nanos.get();
    }

    @Override
    public void sleep(long ms) {
        this.nanos.addAndGet(TimeUnit.NANOSECONDS.convert(ms, TimeUnit.MILLISECONDS));
    }

}
