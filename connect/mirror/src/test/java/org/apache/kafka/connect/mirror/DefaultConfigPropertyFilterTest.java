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

package org.apache.kafka.connect.mirror;

import org.junit.jupiter.api.Test;
import org.apache.kafka.common.config.ConfigException;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.regex.PatternSyntaxException;

class DefaultConfigPropertyFilterTest {

    @Test
    void shouldReplicateConfigProperty_includesUnmatchedByDefault() {
        DefaultConfigPropertyFilter filter = new DefaultConfigPropertyFilter();
        filter.configure(Map.of());
        assertTrue(filter.shouldReplicateConfigProperty("my.unknown.config"), "Unmatched config should be replicated");
    }

    @Test
    void shouldReplicateConfigProperty_excludesPatterns() {
        DefaultConfigPropertyFilter filter = new DefaultConfigPropertyFilter();
        filter.configure(Map.of(DefaultConfigPropertyFilter.CONFIG_PROPERTIES_EXCLUDE_CONFIG, "foo\\..*,bar"));
        assertFalse(filter.shouldReplicateConfigProperty("foo.test"), "Pattern 'foo.*' should exclude config");
        assertFalse(filter.shouldReplicateConfigProperty("bar"), "Pattern 'bar' should exclude config");
        assertTrue(filter.shouldReplicateConfigProperty("baz"), "Config not matching pattern should be allowed");
    }

    @Test
    void shouldReplicateSourceDefault_behavior() {
        DefaultConfigPropertyFilter filter = new DefaultConfigPropertyFilter();
        filter.configure(Map.of(DefaultConfigPropertyFilter.USE_DEFAULTS_FROM, "source"));
        assertTrue(filter.shouldReplicateSourceDefault("any.property"), "Source default flag should be enabled");
    }

    @Test
    void configure_invalidRegexThrows() {
        DefaultConfigPropertyFilter filter = new DefaultConfigPropertyFilter();
        assertThrows(PatternSyntaxException.class, () -> {
            filter.configure(Map.of(DefaultConfigPropertyFilter.CONFIG_PROPERTIES_EXCLUDE_CONFIG, "(***invalid"));
        });
    }
}
