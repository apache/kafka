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
package org.apache.kafka.streams.state.internals;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.Test;


public class JoinSideTest {

    @Test
    public void testMakeLeftSide() {
        final JoinSide leftSide = JoinSide.LEFT;
        final LeftOrRightValue<String, Integer> leftValue = leftSide.make("leftValue");
        assertEquals("leftValue", leftValue.getLeftValue());
        assertNull(leftValue.getRightValue());
    }

    @Test
    public void testMakeRightSide() {
        final JoinSide rightSide = JoinSide.RIGHT;
        final LeftOrRightValue<Integer, String> rightValue = rightSide.make("rightValue");
        assertEquals("rightValue", rightValue.getRightValue());
        assertNull(rightValue.getLeftValue());
    }

    @Test(expected = NullPointerException.class)
    public void testMakeLeftSideWithNull() {
        final JoinSide leftSide = JoinSide.LEFT;
        leftSide.make(null);
    }

    @Test(expected = NullPointerException.class)
    public void testMakeRightSideWithNull() {
        final JoinSide rightSide = JoinSide.RIGHT;
        rightSide.make(null);
    }

    @Test
    public void testIsLeftSide() {
        final JoinSide leftSide = JoinSide.LEFT;
        assertTrue(leftSide.isLeftSide());

        final JoinSide rightSide = JoinSide.RIGHT;
        assertFalse(rightSide.isLeftSide());
    }

    @Test
    public void testToString() {
        final JoinSide leftSide = JoinSide.LEFT;
        assertEquals("left", leftSide.toString());

        final JoinSide rightSide = JoinSide.RIGHT;
        assertEquals("right", rightSide.toString());
    }
}
