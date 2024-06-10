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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KerberosNameTest {

    @Test
    public void testParse() throws IOException {
        List<String> rules = Arrays.asList(
            "RULE:[1:$1](App\\..*)s/App\\.(.*)/$1/g",
            "RULE:[2:$1](App\\..*)s/App\\.(.*)/$1/g",
            "DEFAULT"
        );

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

    @Test
    public void testToLowerCase() throws Exception {
        List<String> rules = Arrays.asList(
            "RULE:[1:$1]/L",
            "RULE:[2:$1](Test.*)s/ABC///L",
            "RULE:[2:$1](ABC.*)s/ABC/XYZ/g/L",
            "RULE:[2:$1](App\\..*)s/App\\.(.*)/$1/g/L",
            "RULE:[2:$1]/L",
            "DEFAULT"
        );

        KerberosShortNamer shortNamer = KerberosShortNamer.fromUnparsedRules("REALM.COM", rules);

        KerberosName name = KerberosName.parse("User@REALM.COM");
        assertEquals("user", shortNamer.shortName(name));

        name = KerberosName.parse("TestABC/host@FOO.COM");
        assertEquals("test", shortNamer.shortName(name));

        name = KerberosName.parse("ABC_User_ABC/host@FOO.COM");
        assertEquals("xyz_user_xyz", shortNamer.shortName(name));

        name = KerberosName.parse("App.SERVICE-name/example.com@REALM.COM");
        assertEquals("service-name", shortNamer.shortName(name));

        name = KerberosName.parse("User/root@REALM.COM");
        assertEquals("user", shortNamer.shortName(name));
    }

    @Test
    public void testToUpperCase() throws Exception {
        List<String> rules = Arrays.asList(
            "RULE:[1:$1]/U",
            "RULE:[2:$1](Test.*)s/ABC///U",
            "RULE:[2:$1](ABC.*)s/ABC/XYZ/g/U",
            "RULE:[2:$1](App\\..*)s/App\\.(.*)/$1/g/U",
            "RULE:[2:$1]/U",
            "DEFAULT"
        );

        KerberosShortNamer shortNamer = KerberosShortNamer.fromUnparsedRules("REALM.COM", rules);

        KerberosName name = KerberosName.parse("User@REALM.COM");
        assertEquals("USER", shortNamer.shortName(name));

        name = KerberosName.parse("TestABC/host@FOO.COM");
        assertEquals("TEST", shortNamer.shortName(name));

        name = KerberosName.parse("ABC_User_ABC/host@FOO.COM");
        assertEquals("XYZ_USER_XYZ", shortNamer.shortName(name));

        name = KerberosName.parse("App.SERVICE-name/example.com@REALM.COM");
        assertEquals("SERVICE-NAME", shortNamer.shortName(name));

        name = KerberosName.parse("User/root@REALM.COM");
        assertEquals("USER", shortNamer.shortName(name));
    }

    @Test
    public void testInvalidRules() {
        testInvalidRule(Arrays.asList("default"));
        testInvalidRule(Arrays.asList("DEFAUL"));
        testInvalidRule(Arrays.asList("DEFAULT/L"));
        testInvalidRule(Arrays.asList("DEFAULT/g"));

        testInvalidRule(Arrays.asList("rule:[1:$1]"));
        testInvalidRule(Arrays.asList("rule:[1:$1]/L/U"));
        testInvalidRule(Arrays.asList("rule:[1:$1]/U/L"));
        testInvalidRule(Arrays.asList("rule:[1:$1]/LU"));
        testInvalidRule(Arrays.asList("RULE:[1:$1/L"));
        testInvalidRule(Arrays.asList("RULE:[1:$1]/l"));
        testInvalidRule(Arrays.asList("RULE:[2:$1](ABC.*)s/ABC/XYZ/L/g"));
    }

    private void testInvalidRule(List<String> rules) {
        assertThrows(
            IllegalArgumentException.class,
            () -> KerberosShortNamer.fromUnparsedRules("REALM.COM", rules));
    }
}
