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
package org.apache.kafka.connect.runtime.errors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Collections;
import java.util.List;

public class ProcessingContext {

    private Stage position;
    private Class<?> klass;
    private ConsumerRecord<byte[], byte[]> consumedMessage;
    private SourceRecord sourceRecord;

    private final List<ErrorReporter> reporters;
    private Result<?> result;
    private long timeOfError;

    private int attempt;

    public ProcessingContext() {
        this(Collections.emptyList());
    }

    public ProcessingContext(List<ErrorReporter> reporters) {
        this.reporters = reporters;
    }

    public ProcessingContext sinkRecord(ConsumerRecord<byte[], byte[]> consumedMessage) {
        this.consumedMessage = consumedMessage;
        return this;
    }

    public ConsumerRecord<byte[], byte[]> sinkRecord() {
        return this.consumedMessage;
    }

    public SourceRecord sourceRecord() {
        return this.sourceRecord;
    }

    public void sourceRecord(SourceRecord record) {
        this.sourceRecord = record;
    }

    public void position(Stage position) {
        this.position = position;
    }

    public Stage stage() {
        return position;
    }

    public Class<?> executingClass() {
        return this.klass;
    }

    public void executingClass(Class<?> klass) {
        this.klass = klass;
    }

    public ProcessingContext result(Result<?> result) {
        this.result = result;
        return this;
    }

    public void setStage(Stage stage, Class<?> klass) {
        position(stage);
        executingClass(klass);
    }

    public void report() {
        for (ErrorReporter reporter: reporters) {
            reporter.report(this);
        }
    }

    public Result result() {
        return this.result;
    }

    @Override
    public String toString() {
        return "ProcessingContext{" +
                "position=" + position +
                ", klass=" + klass +
                ", timeOfError=" + timeOfError +
                ", sourceRecord=" + sourceRecord +
                '}';
    }

    public void setTimeOfError(long timeOfError) {
        this.timeOfError = timeOfError;
    }

    public void attempt(int attempt) {
        this.attempt = attempt;
    }

    public int attempt() {
        return this.attempt;
    }
}
