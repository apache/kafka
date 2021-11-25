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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import org.junit.Test;

public class PositionTest {

    private final String topic = "topic";

    @Test
    public void shouldMatchOnEqual() throws IOException {
        final Position position1 = Position.emptyPosition();
        final Position position2 = Position.emptyPosition();
        position1.update("topic1", 0, 1);
        position2.update("topic1", 0, 1);

        position1.update("topic1", 1, 2);
        position2.update("topic1", 1, 2);

        position1.update("topic1", 2, 1);
        position2.update("topic1", 2, 1);

        position1.update("topic2", 0, 0);
        position2.update("topic2", 0, 0);

        assertEquals(position1, position2);
    }

    @Test
    public void shouldNotMatchOnUnEqual() throws IOException {
        final Position position1 = Position.emptyPosition();
        final Position position2 = Position.emptyPosition();
        position1.update("topic1", 0, 1);
        position2.update("topic1", 0, 1);

        position1.update("topic1", 1, 2);

        position1.update("topic1", 2, 1);
        position2.update("topic1", 2, 1);

        position1.update("topic2", 0, 0);
        position2.update("topic2", 0, 0);

        assertNotEquals(position1, position2);
    }
}
