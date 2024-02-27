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

import org.apache.kafka.common.Uuid;

import java.util.Objects;

/**
 * Application event with a result in the form of a future, that can be retrieved within a
 * timeout based on completion.
 *
 * @param <TYPE>
 */
public abstract class ConsumerEvent<TYPE extends Enum<TYPE>> {

    private final TYPE type;

    private final Uuid id;

    protected ConsumerEvent(TYPE type) {
        this.type = Objects.requireNonNull(type);
        this.id = Uuid.randomUuid();
    }

    public TYPE type() {
        return type;
    }

    public Uuid id() {
        return id;
    }

    @Override
    public final boolean equals(Object o) {
        return this == o;
    }

    @Override
    public final int hashCode() {
        return Objects.hash(type, id);
    }

    protected String toStringBase() {
        return "type=" + type + ", id=" + id;
    }

    @Override
    public final String toString() {
        return getClass().getSimpleName() + "{" +
                toStringBase() +
                '}';
    }
}
