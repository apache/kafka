/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.record;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;


/**
 * this class extends GZIPInputStream and overrides available()
 * in a way that does not return 1 if end of data has been reached
 */
public class KafkaGZIPInputStream extends GZIPInputStream {

    public KafkaGZIPInputStream(InputStream in) throws IOException {
        super(in);
    }

    @Override
    public int available() throws IOException {
        int superResult = super.available();
        if (superResult < 1) {
            return superResult;
        }
        //parent class will return 1 even when inflater is finished.
        //in this case the next read() will return -1. so we ask inflater.
        return inf.finished() ? 0 : 1;
    }
}
