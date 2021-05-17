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
package org.apache.kafka.connect.connector;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigDef;

import java.io.Closeable;
import java.util.Collection;

/**
 * Throttles ConnectRecords based on configurable rate limits.
 *
 */
public interface RateLimiter<R extends ConnectRecord> extends Closeable, Configurable {

    /**
     * Initialize and start rate-limiting based on the provided Time source.
     *
     */
    void start(Time time);

    /**
     * Account for the given recent record batch when subsequently throttling.
     */
    void accumulate(Collection<R> records);

    /**
     * How long to pause (throttle) in order to achieve the desired throughput.
     *
     */
    long throttleTime();

    /** Configuration specification for this RateLimiter */
    ConfigDef config();

    /** Signal that this instance will no longer will be used. */
    @Override
    void close();
}
