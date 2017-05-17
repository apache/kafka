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
package org.apache.kafka.common.security.kerberos;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class KerberosNameTest {

    @Test
    public void testParse() throws IOException {
        List<String> rules = new ArrayList<>(Arrays.asList(
            "RULE:[1:$1](App\\..*)s/App\\.(.*)/$1/g",
            "RULE:[2:$1](App\\..*)s/App\\.(.*)/$1/g",
            "DEFAULT"
        ));
        KerberosShortNamer shortNamer = KerberosShortNamer.fromUnparsedRules("REALM.COM", rules);

        KerberosName name = KerberosName.parse("App.service-name/example.com@REALM.COM");
        assertEquals("App.service-name", name.serviceName());
        assertEquals("example.com", name.hostName());
        assertEquals("REALM.COM", name.realm());
        assertEquals("service-name", shortNamer.shortName(name));

        name = KerberosName.parse("App.service-name@REALM.COM");
        assertEquals("App.service-name", name.serviceName());
        assertNull(name.hostName());
        assertEquals("REALM.COM", name.realm());
        assertEquals("service-name", shortNamer.shortName(name));

        name = KerberosName.parse("user/host@REALM.COM");
        assertEquals("user", name.serviceName());
        assertEquals("host", name.hostName());
        assertEquals("REALM.COM", name.realm());
        assertEquals("user", shortNamer.shortName(name));
    }
}
