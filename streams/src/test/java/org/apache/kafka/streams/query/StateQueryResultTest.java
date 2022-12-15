package org.apache.kafka.streams.query;

import org.apache.kafka.streams.query.internals.SucceededQueryResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;


class StateQueryResultTest {

    StateQueryResult<String> stringStateQueryResult;
    final QueryResult<String> noResultsFound = new SucceededQueryResult<>(null);
    final QueryResult<String> validResult = new SucceededQueryResult<>("Foo");

    @BeforeEach
    public void setUp() {
        stringStateQueryResult = new StateQueryResult<>();
    }

    @Test
    @DisplayName("Zero query results shouldn't error")
    void getOnlyPartitionResultNoResultsTest() {
        stringStateQueryResult.addResult(0, noResultsFound);
        final QueryResult<String> result = stringStateQueryResult.getOnlyPartitionResult();
        assertThat(result, nullValue());
    }

    @Test
    @DisplayName("Valid query results still works")
    void getOnlyPartitionResultWithSingleResultTest() {
        stringStateQueryResult.addResult(0, validResult);
        final QueryResult<String> result = stringStateQueryResult.getOnlyPartitionResult();
        assertThat(result.getResult(), is("Foo"));
    }

    @Test
    @DisplayName("More than one query result throws IllegalArgumentException ")
    void getOnlyPartitionResultMultipleResults() {
        stringStateQueryResult.addResult(0, validResult);
        stringStateQueryResult.addResult(1, validResult);
        assertThrows(IllegalArgumentException.class, () ->stringStateQueryResult.getOnlyPartitionResult());
    }
}