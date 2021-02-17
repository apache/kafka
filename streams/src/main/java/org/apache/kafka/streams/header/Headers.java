package org.apache.kafka.streams.header;

import java.util.Iterator;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

public interface Headers  extends Iterable<Header> {
  /**
   * Get the number of headers in this object.
   *
   * @return the number of headers; never negative
   */
  int size();

  /**
   * Determine whether this object has no headers.
   *
   * @return true if there are no headers, or false if there is at least one header
   */
  boolean isEmpty();

  /**
   * Get the collection of {@link Header} objects whose {@link Header#key() keys} all match the specified key.
   *
   * @param key the key; may not be null
   * @return the iterator over headers with the specified key; may be null if there are no headers with the specified key
   */
  Iterator<Header> allWithName(String key);

  /**
   * Return the last {@link Header} with the specified key.
   *
   * @param key the key for the header; may not be null
   * @return the last Header, or null if there are no headers with the specified key
   */
  Header lastWithName(String key);

  boolean hasWithName(String key);

  /**
   * Add the given {@link Header} to this collection.
   *
   * @param header the header; may not be null
   * @return this object to facilitate chaining multiple methods; never null
   */
  Headers add(Header header);

  /**
   * Removes all {@link Header} objects whose {@link Header#key() key} matches the specified key.
   *
   * @param key the key; may not be null
   * @return this object to facilitate chaining multiple methods; never null
   */
  Headers remove(String key);

  /**
   * Removes all but the latest {@link Header} objects whose {@link Header#key() key} matches the specified key.
   *
   * @param key the key; may not be null
   * @return this object to facilitate chaining multiple methods; never null
   */
  Headers retainLatest(String key);

  /**
   * Removes all but the last {@link Header} object with each key.
   *
   * @return this object to facilitate chaining multiple methods; never null
   */
  Headers retainLatest();

  /**
   * Removes all headers from this object.
   *
   * @return this object to facilitate chaining multiple methods; never null
   */
  Headers clear();

  /**
   * Create a copy of this {@link Headers} object. The new copy will contain all of the same {@link Header} objects as this object.
   * @return the copy; never null
   */
  Headers duplicate();

  /**
   * Get all {@link Header}s, apply the transform to each and store the result in place of the original.
   *
   * @param transform the transform to apply; may not be null
   * @return this object to facilitate chaining multiple methods; never null
   * @throws DataException if the header's value is invalid
   */
  Headers apply(
      Headers.HeaderTransform transform);

  /**
   * Get all {@link Header}s with the given key, apply the transform to each and store the result in place of the original.
   *
   * @param key       the header's key; may not be null
   * @param transform the transform to apply; may not be null
   * @return this object to facilitate chaining multiple methods; never null
   * @throws DataException if the header's value is invalid
   */
  Headers apply(String key, Headers.HeaderTransform transform);

  /**
   * A function to transform the supplied {@link Header}. Implementations will likely need to use {@link Header#with(Schema, Object)}
   * to create the new instance.
   */
  interface HeaderTransform {
    /**
     * Transform the given {@link Header} and return the updated {@link Header}.
     *
     * @param header the input header; never null
     * @return the new header, or null if the supplied {@link Header} is to be removed
     */
    Header apply(Header header);
  }

  org.apache.kafka.common.header.Headers unwrap();
}
