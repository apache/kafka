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

/**
 * Contains all the metadata related to the currently evaluating operation, and associated with a particular
 * sink or source record from the consumer or task, respectively. This class is not thread safe, and so once an
 * instance is passed to a new thread, it should no longer be accessed by the previous thread.
 */
public class ProcessingContext<T> {

    private final T original;

    private Stage position;
    private Class<?> klass;
    private int attempt;
    private Throwable error;

    /**
     * Construct a context associated with the processing of a particular record
     * @param original The original record before processing, as received from either Kafka or a Source Task
     */
    public ProcessingContext(T original) {
        this.original = original;
    }

    /**
     * @return The original record before processing, as received from either Kafka or a Source Task
     */
    public T original() {
        return original;
    }

    /**
     * Set the stage in the connector pipeline which is currently executing.
     *
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
     * A helper method to set both the stage and the class.
     *
     * @param stage the stage
     * @param klass the class which will execute the operation in this stage.
     */
    public void currentContext(Stage stage, Class<?> klass) {
        position(stage);
        executingClass(klass);
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
     * The error (if any) which was encountered while processing the current stage.
     *
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
}
