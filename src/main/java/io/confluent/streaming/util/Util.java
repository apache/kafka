package io.confluent.streaming.util;

import java.util.HashSet;

/**
 * Created by yasuhiro on 6/22/15.
 */
public class Util {

  static <T> HashSet<T> mkSet(T... elems) {
    HashSet<T> set = new HashSet<T>();
    for (T e : elems) set.add(e);
    return set;
  }

}
