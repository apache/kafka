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
package org.apache.kafka.connect.runtime.rest.entities;

import org.apache.kafka.connect.runtime.isolation.PluginDesc;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PluginInfoTest {

    @Test
    public void testNoVersionFilter() {
        PluginInfo.NoVersionFilter filter = new PluginInfo.NoVersionFilter();
        // We intentionally refrain from using assertEquals and assertNotEquals
        // here to ensure that the filter's equals() method is used
        assertFalse(filter.equals("1.0"));
        assertFalse(filter.equals(new Object()));
        assertFalse(filter.equals(null));
        assertTrue(filter.equals(PluginDesc.UNDEFINED_VERSION));
    }
}
