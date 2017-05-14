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
package org.apache.kafka.clients.consumer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ConsumerPair {
    protected final String src;
    protected final String dst;

    public ConsumerPair(String src, String dst) {
        this.src = src;
        this.dst = dst;
    }

    public String toString() {
        return "" + this.src + "->" + this.dst;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.src == null) ? 0 : this.src.hashCode());
        result = prime * result + ((this.dst == null) ? 0 : this.dst.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;

        if (!getClass().isInstance(obj))
            return false;

        ConsumerPair otherPair = (ConsumerPair) obj;
        return this.src.equals(otherPair.src) && this.dst.equals(otherPair.dst);
    }

    public boolean in(Set<ConsumerPair> pairs) {
        for (ConsumerPair pair: pairs)
            if (this.equals(pair))
                return true;
        return false;
    }

    private static boolean isLinked(String src, String dst, Set<ConsumerPair> pairs) {
        if (src.equals(dst))
            return false;

        if (pairs.isEmpty())
            return false;

        if (new ConsumerPair(src, dst).in(pairs))
            return true;

        for (ConsumerPair pair: pairs)
            if (pair.src.equals(src)) {
                Set<ConsumerPair> reducedSet = new HashSet<>(pairs);
                reducedSet.remove(pair);
                if (isLinked(pair.dst, dst, reducedSet))
                    return true;
            }

        return false;
    }

    public static boolean hasCycles(Set<ConsumerPair> pairs) {
        for (ConsumerPair pair: pairs) {
            Set<ConsumerPair> reducedPairs = new HashSet<>(pairs);
            reducedPairs.remove(pair);
            if (isLinked(pair.dst, pair.src, reducedPairs))
                return true;
        }
        return false;
    }

    public static boolean hasReversePairs(Set<ConsumerPair> pairs) {
        int len = pairs.size();
        if (len < 2)
            return false;

        List<ConsumerPair> pairsList = new ArrayList<>(pairs);
        for (int i = 0; i < len - 1; ++i) {
            ConsumerPair pair1 = pairsList.get(i);
            for (int j = i + 1; j < len; ++j) {
                ConsumerPair pair2 = pairsList.get(j);
                if (pair1.src.equals(pair2.dst) && pair2.src.equals(pair1.dst))
                    return true;
            }
        }

        return false;
    }
}
