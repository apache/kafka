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

import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.Punctuator;

public class PunctuationSchedule extends Stamped<ProcessorNode> {

    final long interval;
    final Punctuator punctuator;
    boolean isCancelled = false;
    // this Cancellable will be re-pointed at the successor schedule in next()
    private final RepointableCancellable cancellable;

    public PunctuationSchedule(ProcessorNode node, long interval, Punctuator punctuator) {
        this(node, 0L, interval, punctuator);
    }

    public PunctuationSchedule(ProcessorNode node, long time, long interval, Punctuator punctuator) {
        this(node, time, interval, punctuator, new RepointableCancellable());
        cancellable.setSchedule(this);
    }

    private PunctuationSchedule(ProcessorNode node, long time, long interval, Punctuator punctuator, RepointableCancellable cancellable) {
        super(node, time);
        this.interval = interval;
        this.punctuator = punctuator;
        this.cancellable = cancellable;
    }

    public ProcessorNode node() {
        return value;
    }

    public Punctuator punctuator() {
        return punctuator;
    }

    public Cancellable cancellable() {
        return cancellable;
    }

    public void markCancelled() {
        isCancelled = true;
    }

    public PunctuationSchedule next(long currTimestamp) {
        PunctuationSchedule nextSchedule;
        // we need to special handle the case when it is firstly triggered (i.e. the timestamp
        // is equal to the interval) by reschedule based on the currTimestamp
        if (timestamp == 0L)
            nextSchedule = new PunctuationSchedule(value, currTimestamp + interval, interval, punctuator, cancellable);
        else
            nextSchedule = new PunctuationSchedule(value, timestamp + interval, interval, punctuator, cancellable);

        cancellable.setSchedule(nextSchedule); // update previous cancellable to point to the current schedule

        return nextSchedule;
    }

    private static class RepointableCancellable implements Cancellable {
        private PunctuationSchedule schedule;

        synchronized void setSchedule(PunctuationSchedule schedule) {
            this.schedule = schedule;
        }

        @Override
        synchronized public void cancel() {
            schedule.markCancelled();
        }
    }
}
