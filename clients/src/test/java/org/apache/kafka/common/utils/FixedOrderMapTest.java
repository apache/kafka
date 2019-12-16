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
package org.apache.kafka.common.utils;

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class FixedOrderMapTest {
    @Test
    public void shouldMaintainOrderWhenAdding() {
        final FixedOrderMap<String, Integer> map = new FixedOrderMap<>();
        map.put("a", 0);
        map.put("b", 1);
        map.put("c", 2);
        map.put("b", 3);
        final Iterator<Map.Entry<String, Integer>> iterator = map.entrySet().iterator();
        assertThat(iterator.next(), is(mkEntry("a", 0)));
        assertThat(iterator.next(), is(mkEntry("b", 3)));
        assertThat(iterator.next(), is(mkEntry("c", 2)));
        assertThat(iterator.hasNext(), is(false));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldForbidRemove() {
        final FixedOrderMap<String, Integer> map = new FixedOrderMap<>();
        map.put("a", 0);
        try {
            map.remove("a");
            fail("expected exception");
        } catch (final RuntimeException e) {
            assertThat(e, CoreMatchers.instanceOf(UnsupportedOperationException.class));
        }
        assertThat(map.get("a"), is(0));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldForbidConditionalRemove() {
        final FixedOrderMap<String, Integer> map = new FixedOrderMap<>();
        map.put("a", 0);
        try {
            map.remove("a", 0);
            fail("expected exception");
        } catch (final RuntimeException e) {
            assertThat(e, CoreMatchers.instanceOf(UnsupportedOperationException.class));
        }
        assertThat(map.get("a"), is(0));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldForbidConditionalClear() {
        final FixedOrderMap<String, Integer> map = new FixedOrderMap<>();
        map.put("a", 0);
        try {
            map.clear();
            fail("expected exception");
        } catch (final RuntimeException e) {
            assertThat(e, CoreMatchers.instanceOf(UnsupportedOperationException.class));
        }
        assertThat(map.get("a"), is(0));
    }
}
