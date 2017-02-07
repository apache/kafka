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
 **/
package org.apache.kafka.common.record;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TimestampTypeTest {

    @Test
    public void toAndFromAttributesCreateTime() {
        byte attributes = TimestampType.CREATE_TIME.updateAttributes((byte) 0);
        assertEquals(TimestampType.CREATE_TIME, TimestampType.forAttributes(attributes));
    }

    @Test
    public void toAndFromAttributesLogAppendTime() {
        byte attributes = TimestampType.LOG_APPEND_TIME.updateAttributes((byte) 0);
        assertEquals(TimestampType.LOG_APPEND_TIME, TimestampType.forAttributes(attributes));
    }

    @Test(expected = IllegalArgumentException.class)
    public void updateAttributesNotAllowedForNoTimestampType() {
        TimestampType.NO_TIMESTAMP_TYPE.updateAttributes((byte) 0);
    }

}
