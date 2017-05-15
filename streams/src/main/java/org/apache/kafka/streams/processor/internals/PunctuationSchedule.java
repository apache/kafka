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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.streams.processor.Punctuator;

public class PunctuationSchedule extends Stamped<ProcessorNode> {

    final long interval;
    final Runnable punctuateDelegate;
    boolean isCancelled = false;

    public PunctuationSchedule(ProcessorNode node, long interval, Runnable punctuateDelegate) {
        this(node, 0L, interval, punctuateDelegate);
    }

    public PunctuationSchedule(ProcessorNode node, long time, long interval, Runnable punctuateDelegate) {
        super(node, time);
        this.interval = interval;
        this.punctuateDelegate = punctuateDelegate;
    }

    public ProcessorNode node() {
        return value;
    }

    public Runnable punctuateDelegate() {
        return punctuateDelegate;
    }

    public void cancel() {
        isCancelled = true;
    }

    public PunctuationSchedule next(long currTimestamp) {
        // we need to special handle the case when it is firstly triggered (i.e. the timestamp
        // is equal to the interval) by reschedule based on the currTimestamp
        if (timestamp == 0L)
            return new PunctuationSchedule(value, currTimestamp + interval, interval, punctuateDelegate);
        else
            return new PunctuationSchedule(value, timestamp + interval, interval, punctuateDelegate);
    }

}
