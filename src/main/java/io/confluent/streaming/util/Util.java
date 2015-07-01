package io.confluent.streaming.util;

import java.io.File;
import java.util.HashSet;

/**
 * Created by yasuhiro on 6/22/15.
 */
public class Util {

  public static <T> HashSet<T> mkSet(T... elems) {
    HashSet<T> set = new HashSet<T>();
    for (T e : elems) set.add(e);
    return set;
  }

  /**
   * Recursively delete the given file/directory and any subfiles (if any exist)
   *
   * @param file The root file at which to begin deleting
   */
  public static void rm(File file) {
    if (file == null) {
      return;
    } else if (file.isDirectory()) {
      File[] files = file.listFiles();
      if (files != null) {
        for (File f : files)
          rm(f);
      }
      file.delete();
    } else {
      file.delete();
    }
  }
}
