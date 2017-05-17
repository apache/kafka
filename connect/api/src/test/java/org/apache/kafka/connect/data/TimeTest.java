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
package org.apache.kafka.connect.data;

import org.apache.kafka.connect.errors.DataException;
import org.junit.Test;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;

public class TimeTest {
    private static final GregorianCalendar EPOCH;
    private static final GregorianCalendar EPOCH_PLUS_DATE_COMPONENT;
    private static final GregorianCalendar EPOCH_PLUS_TEN_THOUSAND_MILLIS;
    static {
        EPOCH = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        EPOCH.setTimeZone(TimeZone.getTimeZone("UTC"));

        EPOCH_PLUS_TEN_THOUSAND_MILLIS = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        EPOCH_PLUS_TEN_THOUSAND_MILLIS.setTimeZone(TimeZone.getTimeZone("UTC"));
        EPOCH_PLUS_TEN_THOUSAND_MILLIS.add(Calendar.MILLISECOND, 10000);


        EPOCH_PLUS_DATE_COMPONENT = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        EPOCH_PLUS_DATE_COMPONENT.setTimeZone(TimeZone.getTimeZone("UTC"));
        EPOCH_PLUS_DATE_COMPONENT.add(Calendar.DATE, 10000);
    }

    @Test
    public void testBuilder() {
        Schema plain = Time.SCHEMA;
        assertEquals(Time.LOGICAL_NAME, plain.name());
        assertEquals(1, (Object) plain.version());
    }

    @Test
    public void testFromLogical() {
        assertEquals(0, Time.fromLogical(Time.SCHEMA, EPOCH.getTime()));
        assertEquals(10000, Time.fromLogical(Time.SCHEMA, EPOCH_PLUS_TEN_THOUSAND_MILLIS.getTime()));
    }

    @Test(expected = DataException.class)
    public void testFromLogicalInvalidSchema() {
        Time.fromLogical(Time.builder().name("invalid").build(), EPOCH.getTime());
    }

    @Test(expected = DataException.class)
    public void testFromLogicalInvalidHasDateComponents() {
        Time.fromLogical(Time.SCHEMA, EPOCH_PLUS_DATE_COMPONENT.getTime());
    }

    @Test
    public void testToLogical() {
        assertEquals(EPOCH.getTime(), Time.toLogical(Time.SCHEMA, 0));
        assertEquals(EPOCH_PLUS_TEN_THOUSAND_MILLIS.getTime(), Time.toLogical(Time.SCHEMA, 10000));
    }

    @Test(expected = DataException.class)
    public void testToLogicalInvalidSchema() {
        Time.toLogical(Time.builder().name("invalid").build(), 0);
    }
}
