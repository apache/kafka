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

package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.utils.Bytes;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class WindowKeySchemaTest {

    private final WindowKeySchema windowKeySchema = new WindowKeySchema();

    @Test
    public void boundsSanity() throws Exception {
        // easy
        Bytes upper = windowKeySchema.upperRange(Bytes.wrap(new byte[]{1}), Long.MAX_VALUE);
        assertThat(upper, equalTo(WindowStoreUtils.toBinaryKey(new byte[]{1}, Long.MAX_VALUE, Integer.MAX_VALUE)));

        // hard
        upper = windowKeySchema.upperRange(Bytes.wrap(new byte[]{0, 1}), Long.MAX_VALUE);
        assertThat(upper, equalTo(WindowStoreUtils.toBinaryKey(new byte[]{0}, Long.MAX_VALUE, Integer.MAX_VALUE)));

        upper = windowKeySchema.upperRange(Bytes.wrap(new byte[]{0xC, 0x0}), 0x7fffffffffffffffL);
        assertThat(upper, equalTo(WindowStoreUtils.toBinaryKey(new byte[]{0xC}, Long.MAX_VALUE, Integer.MAX_VALUE)));

        upper = windowKeySchema.upperRange(Bytes.wrap(new byte[]{0xC, 0xC, 0x9}), 0x0AffffffffffffffL);
        assertThat(upper, equalTo(WindowStoreUtils.toBinaryKey(new byte[]{0xC, 0xC}, 0x0AffffffffffffffL, Integer.MAX_VALUE)));


        // easy
        Bytes lower = windowKeySchema.lowerRange(Bytes.wrap(new byte[]{0}), 0);
        assertThat(lower, equalTo(WindowStoreUtils.toBinaryKey(new byte[]{0}, 0, 0)));

        // hard
        lower = windowKeySchema.lowerRange(Bytes.wrap(new byte[]{0}), 1);
        assertThat(lower, equalTo(WindowStoreUtils.toBinaryKey(new byte[]{0}, 0, 0)));

        lower = windowKeySchema.lowerRange(Bytes.wrap(new byte[]{0xf}), 0x7fffffffffffffffL);
        assertThat(lower, equalTo(WindowStoreUtils.toBinaryKey(new byte[]{0xf}, 0, 0)));

        lower = windowKeySchema.lowerRange(Bytes.wrap(new byte[]{0x0, 0xf}), 0x7fffffffffffffffL);
        assertThat(lower, equalTo(WindowStoreUtils.toBinaryKey(new byte[]{0x0, 0xf}, 0, 0)));
    }
}
