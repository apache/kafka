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

import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.Test;
import org.mockito.Mockito;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class StoreQueryUtilsTest {

    @Test
    public void shouldReturnErrorOnNullContext() {
        @SuppressWarnings("unchecked") final KeyQuery<String, Integer> query =
            Mockito.mock(KeyQuery.class);
        @SuppressWarnings("unchecked") final KeyValueStore<String, Integer> store =
            Mockito.mock(KeyValueStore.class);
        final Position position = Position.emptyPosition().withComponent("topic", 0, 1);
        final QueryResult<Integer> queryResult = StoreQueryUtils.handleBasicQueries(
            query,
            PositionBound.at(position),
            new QueryConfig(false),
            store,
            position,
            null
        );
        assertThat(queryResult.isFailure(), is(true));
        assertThat(queryResult.getFailureReason(), is(FailureReason.NOT_UP_TO_BOUND));
        assertThat(
            queryResult.getFailureMessage(),
            is("The store is not initialized yet, so it is not yet up to the bound"
                   + " PositionBound{position=Position{position={topic={0=1}}}}")
        );
    }

    @Test
    public void shouldReturnErrorOnBoundViolation() {
        @SuppressWarnings("unchecked") final KeyQuery<String, Integer> query =
            Mockito.mock(KeyQuery.class);
        @SuppressWarnings("unchecked") final KeyValueStore<String, Integer> store =
            Mockito.mock(KeyValueStore.class);
        final StateStoreContext context = Mockito.mock(StateStoreContext.class);
        Mockito.when(context.taskId()).thenReturn(new TaskId(0, 0));
        final QueryResult<Integer> queryResult = StoreQueryUtils.handleBasicQueries(
            query,
            PositionBound.at(Position.emptyPosition().withComponent("topic", 0, 1)),
            new QueryConfig(false),
            store,
            Position.emptyPosition().withComponent("topic", 0, 0),
            context
        );

        assertThat(queryResult.isFailure(), is(true));
        assertThat(queryResult.getFailureReason(), is(FailureReason.NOT_UP_TO_BOUND));
        assertThat(
            queryResult.getFailureMessage(),
            is("For store partition 0, the current position Position{position={topic={0=0}}}"
                   + " is not yet up to the bound"
                   + " PositionBound{position=Position{position={topic={0=1}}}}")
        );
    }
}
