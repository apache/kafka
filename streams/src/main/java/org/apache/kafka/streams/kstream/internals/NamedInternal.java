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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.kstream.Named;
import java.util.Optional;
import java.util.function.Supplier;

public class NamedInternal extends Named {

    public static NamedInternal empty() {
        return new NamedInternal(null);
    }

    public static NamedInternal with(final String name) {
        return new NamedInternal(name);
    }

    /**
     * Creates a new {@link NamedInternal} instance.
     *
     * @param internal  the internal name.
     */
    NamedInternal(final String internal) {
        super(internal);
    }

    /**
     * @return  a string name.
     */
    public String name() {
        return name;
    }

    @Override
    public NamedInternal withName(final String name) {
        return new NamedInternal(name);
    }

    /**
     * Check whether an internal name is defined.
     * @return {@code false} if no name is set.
     */
    public boolean isDefined() {
        return name != null;
    }

    String suffixWithOrElseGet(final String suffix, final Supplier<String> supplier) {
        final Optional<String> suffixed = Optional.ofNullable(this.name).map(s -> s + suffix);
        // Creating a new named will re-validate generated name as suffixed string could be too large.
        return new NamedInternal(suffixed.orElseGet(supplier)).name();
    }

    String orElseGenerateWithPrefix(final InternalNameProvider provider, final String prefix) {
        return orElseGet(() -> provider.newProcessorName(prefix));
    }

    /**
     * Returns the internal name or the value returns from the supplier.
     *
     * @param supplier  the supplier to be used if internal name is empty.
     * @return an internal string name.
     */
    private String orElseGet(final Supplier<String> supplier) {
        return Optional.ofNullable(this.name).orElseGet(supplier);
    }
}
