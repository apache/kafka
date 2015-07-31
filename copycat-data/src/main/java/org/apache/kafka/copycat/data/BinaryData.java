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
 **/

package org.apache.kafka.copycat.data;


public class BinaryData {
    /**
     * Compares regions of two byte arrays, returning a negative integer, zero, or positive integer when the first byte
     * array region is less than, equal to, or greater than the second byte array region, respectively.
     *
     * @param b1 first byte array
     * @param s1 start of region in first byte array
     * @param l1 length of region in first byte array
     * @param b2 second byte array
     * @param s2 start of region in second byte array
     * @param l2 length of region in second byte array
     * @return a negative integer, zero, or a positive integer as the first byte array is less than, equal to, or greater
     *         than the second byte array
     */
    public static int compareBytes(byte[] b1, int s1, int l1,
                                   byte[] b2, int s2, int l2) {
        int end1 = s1 + l1;
        int end2 = s2 + l2;
        for (int i = s1, j = s2; i < end1 && j < end2; i++, j++) {
            int a = b1[i] & 0xff;
            int b = b2[j] & 0xff;
            if (a != b)
                return a - b;
        }
        return l1 - l2;
    }
}
