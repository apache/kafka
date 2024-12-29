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
package org.apache.kafka.common.security.auth;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

import javax.security.auth.Subject;

/**
 * A simple immutable value object class holding customizable SASL extensions.
 *
 * <p/>
 *
 * <b>Note on object identity and equality</b>: <code>SaslExtensions</code> <em>intentionally</em>
 * overrides the standard {@link #equals(Object)} and {@link #hashCode()} methods calling their
 * respective {@link Object#equals(Object)} and {@link Object#hashCode()} implementations. In so
 * doing, it provides equality <em>only</em> via reference identity and will not base equality on
 * the underlying values of its {@link #extensionsMap extensions map}.
 *
 * <p/>
 *
 * The reason for this approach to equality is based off of the manner in which
 * credentials are stored in a {@link Subject}. <code>SaslExtensions</code> are added to and
 * removed from a {@link Subject} via its {@link Subject#getPublicCredentials() public credentials}.
 * The public credentials are stored in a {@link Set} in the {@link Subject}, so object equality
 * therefore becomes a concern. With shallow, reference-based equality, distinct
 * <code>SaslExtensions</code> instances with the same map values can be considered unique. This is
 * critical to operations like token refresh.
 *
 * See <a href="https://issues.apache.org/jira/browse/KAFKA-14062">KAFKA-14062</a> for more detail.
 */
public class SaslExtensions {
    private final Map<String, String> extensionsMap;

    public SaslExtensions(Map<String, String> extensionsMap) {
        this.extensionsMap = Map.copyOf(extensionsMap);
    }

    /**
     * Returns an <strong>immutable</strong> map of the extension names and their values
     */
    public Map<String, String> map() {
        return extensionsMap;
    }

    /**
     * Creates an "empty" instance indicating no SASL extensions. <em>Do not cache the result of
     * this method call</em> for use by multiple {@link Subject}s as the references need to be
     * unique.
     *
     * <p/>
     *
     * See the class-level documentation for details.
     * @return Unique, but empty, <code>SaslExtensions</code> instance
     */
    @SuppressWarnings("unchecked")
    public static SaslExtensions empty() {
        // It's ok to re-use the EMPTY_MAP instance as the object equality is on the outer
        // SaslExtensions reference.
        return new SaslExtensions(Collections.EMPTY_MAP);
    }

    /**
     * Implements equals using the reference comparison implementation from
     * {@link Object#equals(Object)}.
     *
     * <p/>
     *
     * See the class-level documentation for details.
     *
     * @param o Other object to compare
     * @return True if <code>o == this</code>
     */
    @Override
    public final boolean equals(Object o) {
        return super.equals(o);
    }

    /**
     * Implements <code>hashCode</code> using the native implementation from
     * {@link Object#hashCode()}.
     *
     * <p/>
     *
     * See the class-level documentation for details.
     *
     * @return Hash code of instance
     */
    @Override
    public final int hashCode() {
        return super.hashCode();
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", SaslExtensions.class.getSimpleName() + "[", "]")
            .add("extensionsMap=" + extensionsMap)
            .toString();
    }

}
