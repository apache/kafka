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
package org.apache.kafka.clients.consumer.internals.events;

/**
 * This is the abstract definition of the events created by the KafkaConsumer API
 */
abstract public class ApplicationEvent {
    public final Type type;

    protected ApplicationEvent(Type type) {
        this.type = type;
    }
    /**
     * process the application event. Return true upon succesful execution,
     * false otherwise.
     * @return true if the event was successfully executed; false otherwise.
     */

    @Override
    public String toString() {
        return type + " ApplicationEvent";
    }
    public enum Type {
        NOOP, COMMIT,
    }
}
