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

package org.apache.kafka.clients.admin;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class ConfigTest {
    private static final ConfigEntry E1 = new ConfigEntry("a", "b");
    private static final ConfigEntry E2 = new ConfigEntry("c", "d");
    private Config config;

    @Before
    public void setUp() {
        final Collection<ConfigEntry> entries = new ArrayList<>();
        entries.add(E1);
        entries.add(E2);

        config = new Config(entries);
    }

    @Test
    public void shouldGetEntry() {
        assertThat(config.get("a"), is(E1));
        assertThat(config.get("c"), is(E2));
    }

    @Test
    public void shouldReturnNullOnGetUnknownEntry() {
        assertThat(config.get("unknown"), is(nullValue()));
    }

    @Test
    public void shouldGetAllEntries() {
        assertThat(config.entries().size(), is(2));
        assertThat(config.entries(), hasItems(E1, E2));
    }

    @Test
    public void shouldImplementEqualsProperly() {
        final Collection<ConfigEntry> entries = new ArrayList<>();
        entries.add(E1);

        assertThat(config, is(equalTo(config)));
        assertThat(config, is(equalTo(new Config(config.entries()))));
        assertThat(config, is(not(equalTo(new Config(entries)))));
        assertThat(config, is(not(equalTo((Object) "this"))));
    }

    @Test
    public void shouldImplementHashCodeProperly() {
        final Collection<ConfigEntry> entries = new ArrayList<>();
        entries.add(E1);

        assertThat(config.hashCode(), is(config.hashCode()));
        assertThat(config.hashCode(), is(new Config(config.entries()).hashCode()));
        assertThat(config.hashCode(), is(not(new Config(entries).hashCode())));
    }

    @Test
    public void shouldImplementToStringProperly() {
        assertThat(config.toString(), containsString(E1.toString()));
        assertThat(config.toString(), containsString(E2.toString()));
    }
}
