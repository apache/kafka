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

package org.apache.kafka.common.compress;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class SnappyFactory {

    private SnappyFactory() { }

    public static OutputStream wrapForOutput(ByteBufferOutputStream buffer) {
        try {
            return new SnappyOutputStream(buffer);
        } catch (Throwable e) {
            throw new KafkaException(e);
        }
    }

    public static InputStream wrapForInput(ByteBuffer buffer) {
        try {
            return new SnappyInputStream(new ByteBufferInputStream(buffer));
        } catch (Throwable e) {
            throw new KafkaException(e);
        }
    }

}
