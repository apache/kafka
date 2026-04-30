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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

public class ShareAcknowledgementMode {
    public enum AcknowledgementMode {
        IMPLICIT, EXPLICIT;

        @Override
        public String toString() {
            return super.toString().toLowerCase(Locale.ROOT);
        }
    }

    private final AcknowledgementMode acknowledgementMode;

    public static final ShareAcknowledgementMode IMPLICIT = new ShareAcknowledgementMode(AcknowledgementMode.IMPLICIT);
    public static final ShareAcknowledgementMode EXPLICIT = new ShareAcknowledgementMode(AcknowledgementMode.EXPLICIT);

    private ShareAcknowledgementMode(AcknowledgementMode acknowledgementMode) {
        this.acknowledgementMode = acknowledgementMode;
    }

    /**
     * Returns the ShareAcknowledgementMode from the given string.
     */
    public static ShareAcknowledgementMode fromString(String acknowledgementMode) {
        if (acknowledgementMode == null) {
            throw new IllegalArgumentException("Acknowledgement mode is null");
        }

        if (Arrays.asList(Utils.enumOptions(AcknowledgementMode.class)).contains(acknowledgementMode)) {
            AcknowledgementMode mode = AcknowledgementMode.valueOf(acknowledgementMode.toUpperCase(Locale.ROOT));
            switch (mode) {
                case IMPLICIT:
                    return IMPLICIT;
                case EXPLICIT:
                    return EXPLICIT;
                default:
                    throw new IllegalArgumentException("Invalid acknowledgement mode: " + acknowledgementMode);
            }
        } else {
            throw new IllegalArgumentException("Invalid acknowledgement mode: " + acknowledgementMode);
        }
    }

    /**
     * Returns the name of the acknowledgement mode.
     */
    public String name() {
        return acknowledgementMode.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShareAcknowledgementMode that = (ShareAcknowledgementMode) o;
        return acknowledgementMode == that.acknowledgementMode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(acknowledgementMode);
    }

    @Override
    public String toString() {
        return "ShareAcknowledgementMode{" +
                "mode=" + acknowledgementMode +
                '}';
    }

    public static class Validator implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object value) {
            String acknowledgementMode = (String) value;
            try {
                fromString(acknowledgementMode);
            } catch (Exception e) {
                throw new ConfigException(name, value, "Invalid value `" + acknowledgementMode + "` for configuration " +
                        name + ". The value must either be 'implicit' or 'explicit'.");
            }
        }

        @Override
        public String toString() {
            String values = Arrays.stream(ShareAcknowledgementMode.AcknowledgementMode.values())
                    .map(ShareAcknowledgementMode.AcknowledgementMode::toString).collect(Collectors.joining(", "));
            return "[" + values + "]";
        }
    }
}
