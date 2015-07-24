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
    public static int compareBytes(byte[] b1, int s1, int l1,
                                   byte[] b2, int s2, int l2) {
        int end1 = s1 + l1;
        int end2 = s2 + l2;
        for (int i = s1, j = s2; i < end1 && j < end2; i++, j++) {
            int a = b1[i] & 0xff;
            int b = b2[j] & 0xff;
            if (a != b) {
                return a - b;
            }
        }
        return l1 - l2;
    }
}
