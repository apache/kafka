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

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class ConfigTest {

    @Test
    public void shouldImplementEqualsProperly() {
        final ConfigEntry e1 = new ConfigEntry("a", "b");
        final ConfigEntry e2 = new ConfigEntry("c", "d");

        final Collection<ConfigEntry> entries1 = new ArrayList<>();
        entries1.add(e1);

        final Collection<ConfigEntry> entries2 = new ArrayList<>();
        entries2.add(e1);
        entries2.add(e2);

        assertThat(new Config(entries1), is(equalTo(new Config(entries1))));
        assertThat(new Config(entries1), is(not(equalTo(new Config(entries2)))));
    }

    @Test
    public void shouldImplementHashCodeProperly() {
        final ConfigEntry e1 = new ConfigEntry("a", "b");
        final ConfigEntry e2 = new ConfigEntry("c", "d");

        final Collection<ConfigEntry> entries1 = new ArrayList<>();
        entries1.add(e1);

        final Collection<ConfigEntry> entries2 = new ArrayList<>();
        entries2.add(e1);
        entries2.add(e2);

        assertThat(new Config(entries1).hashCode(), is(new Config(entries1).hashCode()));
        assertThat(new Config(entries1).hashCode(), is(not(new Config(entries2).hashCode())));
    }
}
