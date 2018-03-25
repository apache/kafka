package org.apache.kafka.streams

import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.common.utils.Bytes

package object scala {
  type ByteArrayKVStore = KeyValueStore[Bytes, Array[Byte]]
}
