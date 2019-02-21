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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.streams.errors.InvalidProcessorException;

import java.util.Objects;

public class Named implements NamedOperation<Named> {

    protected String name;

    protected Named(final Named named) {
        this(Objects.requireNonNull(named, "named can't be null").name);
    }

    protected Named(final String name) {
        this.name = name;
        if (name != null) {
            // Validate the specified processor name.
            try {
                Topic.validate(name);
            } catch (final InvalidTopicException e) {
                throw new InvalidProcessorException("Invalid processor name", e);
            }
        }
    }

    /**
     * Creates a Named instance with empty name.
     */
    protected Named() {
    }

    /**
     * Create a Named instance with provided name.
     *
     * @param name  the processor name to be used. If {@code null} a default processor name will be generated.
     * @return      A new {@link Named} instance configured with name
     */
    public static Named as(final String name) {
        Objects.requireNonNull(name, "name can't be null");
        return new Named(name);
    }

    @Override
    public Named withName(final String name) {
        Objects.requireNonNull(name, "name can't be null");
        return new Named(name);
    }
}
