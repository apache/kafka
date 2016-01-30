/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.utils;

import java.util.zip.CRC32;

/**
 * A helper class to compute CRC32.
 * @see java.util.zip.CRC32
 */
public class Crc32 extends CRC32 {

    /**
     * Update the CRC32 given an integer
     */
    final public void updateInt(int input) {
        update((byte) (input >> 24));
        update((byte) (input >> 16));
        update((byte) (input >> 8));
        update((byte) input /* >> 0 */);
    }

}
