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

    private final long interval;
    private final Punctuator punctuator;
    private boolean isCancelled = false;
    // this Cancellable will be re-pointed at the successor schedule in next()
    private final RepointableCancellable cancellable;

    PunctuationSchedule(ProcessorNode node, long interval, Punctuator punctuator) {
        this(node, 0L, interval, punctuator, new RepointableCancellable());
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

    void markCancelled() {
        isCancelled = true;
    }

    boolean isCancelled() {
        return isCancelled;
    }

    public PunctuationSchedule next(final long currTimestamp) {
        long nextPunctuationTime;
        // avoid scheduling a new punctuations immediately, this can happen:
        // - when using STREAM_TIME punctuation and there was a gap i.e., no data was
        //   received for more than 2*interval (also happens on first STREAM_TIME punctuation i.e., when timestamp == 0L)
        // - when using WALL_CLOCK_TIME and there was a gap i.e., punctuation was delayed for more than 2*interval (GC pause, overload, ...)
        if (timestamp + interval < currTimestamp) {
            nextPunctuationTime = currTimestamp + interval;
        } else {
            nextPunctuationTime = timestamp + interval;
        }

        PunctuationSchedule nextSchedule = new PunctuationSchedule(value, nextPunctuationTime, interval, punctuator, cancellable);

        cancellable.setSchedule(nextSchedule);

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
