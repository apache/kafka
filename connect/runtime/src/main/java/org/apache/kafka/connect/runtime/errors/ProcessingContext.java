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

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * Contains all the metadata related to the currently evaluating operation.
 */
class ProcessingContext {

    private Collection<ErrorReporter> reporters = Collections.emptyList();

    private ConsumerRecord<byte[], byte[]> consumedMessage;
    private SourceRecord sourceRecord;

    /**
     * the following fields need to be reset every time a new record is seen.
     */

    private Stage position;
    private Class<?> klass;
    private int attempt;
    private Throwable error;

    /**
     * Only one instance of this class is meant to exist per task in a JVM. This method resets the internal fields
     * for every new record that is about to be processed.
     */
    private void reset() {
        attempt = 0;
        position = null;
        klass = null;
        error = null;
    }

    /**
     * set the record consumed from Kafka in a sink connector.
     * @param consumedMessage the record
     */
    public void consumerRecord(ConsumerRecord<byte[], byte[]> consumedMessage) {
        this.consumedMessage = consumedMessage;
        reset();
    }

    /**
     * @return the record consumed from Kafka. could be null
     */
    public ConsumerRecord<byte[], byte[]> consumerRecord() {
        return consumedMessage;
    }

    /**
     * @return the source record being processed.
     */
    public SourceRecord sourceRecord() {
        return sourceRecord;
    }

    /**
     * Set the source record being processed in the connect pipeline.
     * @param record the source record
     */
    public void sourceRecord(SourceRecord record) {
        this.sourceRecord = record;
        reset();
    }

    /**
     * set the stage in the connector pipeline which is currently executing.
     * @param position the stage
     */
    public void position(Stage position) {
        this.position = position;
    }

    /**
     * @return the stage in the connector pipeline which is currently executing.
     */
    public Stage stage() {
        return position;
    }

    /**
     * @return the class which is going to execute the current operation.
     */
    public Class<?> executingClass() {
        return klass;
    }

    /**
     * @param klass set the class which is currently executing.
     */
    public void executingClass(Class<?> klass) {
        this.klass = klass;
    }

    /**
     * a helper method to set both the stage and the class which
     * @param stage the stage
     * @param klass the class which will execute the operation in this stage.
     */
    public void setCurrentContext(Stage stage, Class<?> klass) {
        position(stage);
        executingClass(klass);
    }

    /**
     * Report errors. Should be called only if an error was encountered while executing the operation.
     */
    public void report() {
        for (ErrorReporter reporter: reporters) {
            reporter.report(this);
        }
    }

    @Override
    public String toString() {
        return "ProcessingContext{" +
                "position=" + position +
                ", class=" + klass +
                ", consumedMessage=" + consumedMessage +
                ", sourceRecord=" + sourceRecord +
                ", attempt=" + attempt +
                ", reporters=" + reporters +
                '}';
    }

    /**
     * @param attempt the number of attempts made to execute the current operation.
     */
    public void attempt(int attempt) {
        this.attempt = attempt;
    }

    /**
     * @return the number of attempts made to execute the current operation.
     */
    public int attempt() {
        return attempt;
    }

    /**
     * @return the error (if any) which was encountered while processing the current stage.
     */
    public Throwable error() {
        return error;
    }

    /**
     * the error (if any) which was encountered while processing the current stage.
     * @param error the error
     */
    public void error(Throwable error) {
        this.error = error;
    }

    /**
     * @return true, if the last operation encountered an error; false otherwise
     */
    public boolean failed() {
        return error() != null;
    }

    public void setReporters(Collection<ErrorReporter> reporters) {
        Objects.requireNonNull(reporters);
        this.reporters = reporters;
    }

}
