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

import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.utils.Base64.Decoder;
import org.apache.kafka.common.utils.Base64.Encoder;
import org.junit.Test;

public class Base64Test {

    @Test
    public void testBase64UrlEncodeDecode() {
        confirmInversesForAllThreePaddingCases(Base64.urlEncoderNoPadding(), Base64.urlDecoder());
    }

    @Test
    public void testBase64EncodeDecode() {
        confirmInversesForAllThreePaddingCases(Base64.encoder(), Base64.decoder());
    }

    private static void confirmInversesForAllThreePaddingCases(Encoder encoder, Decoder decoder) {
        for (String text : new String[] {"", "a", "ab", "abc"}) {
            assertEquals(text, new String(decoder.decode(encoder.encodeToString(text.getBytes(StandardCharsets.UTF_8))),
                    StandardCharsets.UTF_8));
        }
    }
}
