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

package org.apache.kafka.metadata.authorizer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetAddress;

import static org.apache.kafka.metadata.authorizer.CidrUtils.isInRange;
import static org.apache.kafka.metadata.authorizer.CidrUtils.validate;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class CidrUtilsTest {

    @Test
    public void testValidateValidIpv4Cidr() {
        assertDoesNotThrow(() -> validate("192.168.0.0/24"));
        assertDoesNotThrow(() -> validate("10.0.0.0/8"));
        assertDoesNotThrow(() -> validate("172.16.0.0/16"));
        assertDoesNotThrow(() -> validate("192.168.1.1/32"));
        assertDoesNotThrow(() -> validate("0.0.0.0/0"));
    }

    @Test
    public void testValidateValidIpv6Cidr() {
        assertDoesNotThrow(() -> validate("2001:db8::/32"));
        assertDoesNotThrow(() -> validate("2001:db8:abcd::/48"));
        assertDoesNotThrow(() -> validate("::1/128"));
        assertDoesNotThrow(() -> validate("::/0"));
    }

    @Test
    public void testValidateInvalidIpv4Cidr() {
        assertThrows(IllegalArgumentException.class, () -> validate("192.168.0.0/33"));
        assertThrows(IllegalArgumentException.class, () -> validate("192.168.0.0/-1"));
        assertThrows(IllegalArgumentException.class, () -> validate("192.168.0.256/24"));
        assertThrows(IllegalArgumentException.class, () -> validate("192.168.0.0/abc"));
        assertThrows(IllegalArgumentException.class, () -> validate("192.168.0.0/"));
    }

    @Test
    public void testValidateInvalidIpv6Cidr() {
        assertThrows(IllegalArgumentException.class, () -> validate("2001:db8::/129"));
    }

    @Test
    public void testValidateIpv4MappedIpv6Cidr() {
        // ::ffff:x.x.x.x/N contains ':', so isIpv6() returns true and SubnetUtils6 handles it.
        assertThrows(IllegalArgumentException.class, () -> validate("::ffff:192.168.1.0/24"));
    }

    @Test
    public void testValidateHostnameWithPath() {
        assertThrows(IllegalArgumentException.class, () -> validate("example.com/test"));
    }

    @Test
    public void testIsInRangeNullOrNonCidr() {
        assertFalse(isInRange("192.168.1.1", null));
        assertFalse(isInRange("192.168.1.1", "192.168.1.1"));
    }

    @Test
    public void testIpv4MappedAddress() throws Exception {
        // JVM normalizes ::ffff:x.x.x.x to plain IPv4 Inet4Address,
        // so CidrUtils dispatches to SubnetUtils (IPv4 path), not SubnetUtils6.
        String normalizedHost = InetAddress.getByName("::ffff:192.168.1.5").getHostAddress();
        assertEquals("192.168.1.5", normalizedHost);
        assertTrue(isInRange(normalizedHost, "192.168.1.0/24"));
        assertFalse(isInRange(normalizedHost, "10.0.0.0/8"));
    }

    @Test
    public void testIpv4MappedAddressWithSubnet() {
        // ::ffff:x.x.x.x/N contains ':', so isIpv6() returns true and SubnetUtils6 handles it.
        // Plain IPv4 hosts don't match this IPv6 CIDR range.
        assertFalse(isInRange("192.168.1.5", "::ffff:192.168.1.0/24"));
        assertFalse(isInRange("::1", "::ffff:192.168.1.0/24"));
        assertFalse(isInRange("2001:db8::1", "::ffff:192.168.1.0/24"));

        // ::x.x.x.x/N (deprecated IPv4-compatible form): JVM keeps it as Inet6Address,
        // so CidrUtils dispatches to SubnetUtils6. Plain IPv4 clients won't match.
        assertFalse(isInRange("192.168.1.5", "::192.168.1.0/24"));
        // IPv6 clients within the /24 range do match.
        assertTrue(isInRange("::1", "::192.168.1.0/24"));
        assertTrue(isInRange("0:0:0:0:0:0:c0a8:105", "::192.168.1.0/24"));
        // IPv6 clients outside the /24 range do not match.
        assertFalse(isInRange("2001:db8::1", "::192.168.1.0/24"));
    }
}
