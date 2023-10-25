package org.apache.kafka.streams.query;

import java.time.Instant;
import java.util.Optional;
import org.apache.kafka.common.annotation.InterfaceStability.Evolving;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.VersionedRecord;

/**
 * Interactive query for retrieving a set of records with keys within a specified key range and time
 * range.
 */

@Evolving
public final class MultiVersionedRangeQuery<K, V> implements
    Query<KeyValueIterator<K, VersionedRecord<V>>> {

  private final Optional<K> lower;
  private final Optional<K> upper;
  private final Optional<Instant> fromTime;

  private final Optional<Instant> toTime;
  private final boolean isKeyAscending;
  private final boolean isTimeAscending;
  private final boolean isOrderedByKey;

  private MultiVersionedRangeQuery(final Optional<K> lower, final Optional<K> upper, final Optional<Instant> fromTime, final Optional<Instant> toTime, final boolean isOrderedByKey, final boolean isKeyAscending, final boolean isTimeAscending) {
      this.lower = lower;
      this.upper = upper;
      this.fromTime = fromTime;
      this.toTime = toTime;
      this.isOrderedByKey = isOrderedByKey;
      this.isKeyAscending = isKeyAscending;
      this.isTimeAscending = isTimeAscending;
  }

  /**
   * Interactive range query using a lower and upper bound to filter the keys returned. * For each
   * key the records valid within the specified time range are returned. * In case the time range is
   * not specified just the latest record for each key is returned.
   * @param lower The key that specifies the lower bound of the range
   * @param upper The key that specifies the upper bound of the range
   * @param <K> The key type
   * @param <V> The value type
   */
  public static <K, V> MultiVersionedRangeQuery<K, V> withKeyRange(final K lower, final K upper){
      return new MultiVersionedRangeQuery<>(Optional.of(lower), Optional.of(upper), Optional.empty(), Optional.empty(), true, true, true);
  }


  /**
   * Interactive range query using a lower bound to filter the keys returned. * For each key the
   * records valid within the specified time range are returned. * In case the time range is not
   * specified just the latest record for each key is returned.
   * @param lower The key that specifies the lower bound of the range
   * @param <K>   The key type
   * @param <V>   The value type
   */
  public static <K, V> MultiVersionedRangeQuery<K, V> withLowerKeyBound(final K lower) {
      return new MultiVersionedRangeQuery<>(Optional.of(lower), Optional.empty(), Optional.empty(), Optional.empty(), true, true, true);
  }

  /**
   * Interactive range query using a lower bound to filter the keys returned. * For each key the
   * records valid within the specified time range are returned. * In case the time range is not
   * specified just the latest record for each key is returned.
   * @param upper The key that specifies the lower bound of the range
   * @param <K>   The key type
   * @param <V>   The value type
   */
  public static <K, V> MultiVersionedRangeQuery<K, V> withUpperKeyBound(final K upper) {
      return new MultiVersionedRangeQuery<>(Optional.empty(), Optional.of(upper), Optional.empty(), Optional.empty(), true, true, true);
  }

  /**
   * Interactive scan query that returns all records in the store. * For each key the records valid
   * within the specified time range are returned. * In case the time range is not specified just
   * the latest record for each key is returned.
   * @param <K> The key type
   * @param <V> The value type
   */
  public static <K, V> MultiVersionedRangeQuery<K, V> allKeys() {
      return new MultiVersionedRangeQuery<>(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), true, true, true);
  }

  /**
   * Specifies the starting time point for the key query. The range query returns all the records
   * that are valid in the time range starting from the timestamp {@code fromTimestamp}.
   * @param fromTime The starting time point
   */
  public MultiVersionedRangeQuery<K, V> fromTime(Instant fromTime) {
      if (fromTime == null) {
          return new MultiVersionedRangeQuery<>(lower, upper, Optional.empty(), toTime, isOrderedByKey, isKeyAscending, isTimeAscending);
      }
      return new MultiVersionedRangeQuery<>(lower, upper, Optional.of(fromTime), toTime, isOrderedByKey, isKeyAscending, isTimeAscending);
  }

  /**
   * Specifies the ending time point for the key query. The range query returns all the records that
   * have timestamp <= {@code asOfTimestamp}.
   * @param toTime The ending time point
   */
  public MultiVersionedRangeQuery<K, V> toTime(Instant toTime) {
      if (toTime == null) {
          return new MultiVersionedRangeQuery<>(lower, upper, fromTime, Optional.empty(), isOrderedByKey, isKeyAscending, isTimeAscending);
      }
      return new MultiVersionedRangeQuery<>(lower, upper, fromTime, Optional.of(toTime), isOrderedByKey, isKeyAscending, isTimeAscending);
  }

  /**
   * Specifies the overall order of returned records by key
   */
  public MultiVersionedRangeQuery<K, V> orderByKey() {
    return new MultiVersionedRangeQuery<>(lower, upper, fromTime, toTime, true, isKeyAscending, isTimeAscending);
  }

  /**
   * Specifies the overall order of returned records by timestamp
   */
  public MultiVersionedRangeQuery<K, V> orderByTimestamp() {
      return new MultiVersionedRangeQuery<>(lower, upper, fromTime, toTime, false, isKeyAscending, isTimeAscending);
  }

  /**
   * Specifies the order of keys as ascending.
   */
  public MultiVersionedRangeQuery<K, V> withAscendingKeys() {
    return new MultiVersionedRangeQuery<>(lower, upper, fromTime, toTime, isOrderedByKey, true, isTimeAscending);
  }

  /**
   * Specifies the order of keys as descending.
   */
  public MultiVersionedRangeQuery<K, V> withDescendingKeys() {
      return new MultiVersionedRangeQuery<>(lower, upper, fromTime, toTime, isOrderedByKey, false, isTimeAscending);
  }

  /**
   * Specifies the order of the timestamps as ascending.
   */
  public MultiVersionedRangeQuery<K, V> withAscendingTimestamps() {
    return new MultiVersionedRangeQuery<>(lower, upper, fromTime, toTime, isOrderedByKey, isKeyAscending, true);
  }

  /**
   * Specifies the order of the timestamps as descending.
   */
  public MultiVersionedRangeQuery<K, V> withDescendingTimestamps() {
      return new MultiVersionedRangeQuery<>(lower, upper, fromTime, toTime, isOrderedByKey, isKeyAscending, false);
  }

  /**
   * The lower bound of the query, if specified.
   */
  public Optional<K> lowerKeyBound() {
      return lower;
  }

  /**
   * The upper bound of the query, if specified
   */
  public Optional<K> upperKeyBound() {
      return upper;
  }

  /**
   * The starting time point of the query, if specified
   */
  public Optional<Instant> fromTime() {
    return fromTime;
  }

  /**
   * The ending time point of the query, if specified
   */
  public Optional<Instant> toTime() {
    return toTime;
  }

  /**
   * @return true if the query orders the returned records by key
   */
  public boolean isOrderedByKey() {
    return isOrderedByKey;
  }

  /**
   * @return true if the query returns records in ascending order of keys
   */
  public boolean isKeyAscending() {
    return isKeyAscending;
  }

  /**
   * @return true if the query returns records in ascending order of timestamps
   */
  public boolean isTimeAscending() {
    return isTimeAscending;
  }

}