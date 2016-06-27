/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.security.auth;

import org.junit.Assert;
import org.junit.Test;

public class KafkaPrincipalTest {

    @Test
    public void testPrincipalNameCanContainSeparator() {
        String name = "name" + KafkaPrincipal.SEPARATOR + "with" + KafkaPrincipal.SEPARATOR + "in" + KafkaPrincipal.SEPARATOR + "it";

        KafkaPrincipal principal = KafkaPrincipal.fromString(KafkaPrincipal.USER_TYPE + KafkaPrincipal.SEPARATOR + name);
        Assert.assertEquals(KafkaPrincipal.USER_TYPE, principal.getPrincipalType());
        Assert.assertEquals(name, principal.getName());
    }

    @Test
    public void testEqualsAndHashCode() {
        String name = "KafkaUser";
        KafkaPrincipal principal1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, name);
        KafkaPrincipal principal2 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, name);

        Assert.assertEquals(principal1.hashCode(), principal2.hashCode());
        Assert.assertEquals(principal1, principal2);
    }
}
