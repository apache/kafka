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
package org.apache.kafka.clients.consumer;

import java.io.Serializable;

/**
 * Key range for records. Used in concurrent offset commit.
 */
public class RecordKeyRange implements Serializable {
    private long low;
    private long hi;

    public RecordKeyRange(long low, long hi) {
        this.low = low;
        this.hi = hi;
    }

    public long lowerBound() {
        return low;
    }

    public long upperBound() {
        return hi;
    }
}
