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

import org.apache.kafka.streams.errors.TopologyException;

import java.util.Objects;

public class Named implements NamedOperation<Named> {

    private static final int MAX_NAME_LENGTH = 249;

    protected String name;

    protected Named(final Named named) {
        this(Objects.requireNonNull(named, "named can't be null").name);
    }

    protected Named(final String name) {
        this.name = name;
        if (name != null) {
            validate(name);
        }
    }

    /**
     * Create a Named instance with provided name.
     *
     * @param name  the processor name to be used. If {@code null} a default processor name will be generated.
     * @return      A new {@link Named} instance configured with name
     *
     * @throws TopologyException if an invalid name is specified; valid characters are ASCII alphanumerics, '.', '_' and '-'.
     */
    public static Named as(final String name) {
        Objects.requireNonNull(name, "name can't be null");
        return new Named(name);
    }

    @Override
    public Named withName(final String name) {
        return new Named(name);
    }

    protected static void validate(final String name) {
        if (name.isEmpty())
            throw new TopologyException("Name is illegal, it can't be empty");
        if (name.equals(".") || name.equals(".."))
            throw new TopologyException("Name cannot be \".\" or \"..\"");
        if (name.length() > MAX_NAME_LENGTH)
            throw new TopologyException("Name is illegal, it can't be longer than " + MAX_NAME_LENGTH +
                    " characters, name: " + name);
        if (!containsValidPattern(name))
            throw new TopologyException("Name \"" + name + "\" is illegal, it contains a character other than " +
                    "ASCII alphanumerics, '.', '_' and '-'");
    }

    /**
     * Valid characters for Kafka topics are the ASCII alphanumerics, '.', '_', and '-'
     */
    private static boolean containsValidPattern(final String topic) {
        for (int i = 0; i < topic.length(); ++i) {
            final char c = topic.charAt(i);

            // We don't use Character.isLetterOrDigit(c) because it's slower
            final boolean validLetterOrDigit = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z');
            final boolean validChar = validLetterOrDigit || c == '.' || c == '_' || c == '-';
            if (!validChar) {
                return false;
            }
        }
        return true;
    }
}
