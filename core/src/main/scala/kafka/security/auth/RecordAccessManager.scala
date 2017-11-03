package kafka.security.auth

import java.io.IOException
import java.nio.channels.GatheringByteChannel

import org.apache.kafka.common.record.Records
import org.apache.kafka.common.utils.Time

class RecordAccessManager(val originalRecords: Records, recordAuthorizer: RecordAuthorizer) extends Records{
  /**
    * The size of these records in bytes.
    *
    * @return The size in bytes of the records
    */
  override def sizeInBytes() = originalRecords.sizeInBytes()

  /**
    * Attempts to write the contents of this buffer to a channel.
    *
    * @param channel  The channel to write to
    * @param position The position in the buffer to write from
    * @param length   The number of bytes to write
    * @return The number of bytes actually written
    * @throws IOException For any IO errors
    */
  override def writeTo(channel: GatheringByteChannel, position: Long, length: Int) =
    originalRecords.writeTo(channel, position, length)

  /**
    * Get the record batches. Note that the signature allows subclasses
    * to return a more specific batch type. This enables optimizations such as in-place offset
    * assignment (see for example {@link DefaultRecordBatch}), and partial reading of
    * record data (see {@link FileLogInputStream.FileChannelRecordBatch#magic()}.
    *
    * @return An iterator over the record batches of the log
    */
  override def batches() = originalRecords.batches()

  /**
    * Check whether all batches in this buffer have a certain magic value.
    *
    * @param magic The magic value to check
    * @return true if all record batches have a matching magic value, false otherwise
    */
  override def hasMatchingMagic(magic: Byte) = originalRecords.hasCompatibleMagic(magic)

  /**
    * Check whether this log buffer has a magic value compatible with a particular value
    * (i.e. whether all message sets contained in the buffer have a matching or lower magic).
    *
    * @param magic The magic version to ensure compatibility with
    * @return true if all batches have compatible magic, false otherwise
    */
  override def hasCompatibleMagic(magic: Byte) = originalRecords.hasCompatibleMagic(magic)

  /**
    * Convert all batches in this buffer to the format passed as a parameter. Note that this requires
    * deep iteration since all of the deep records must also be converted to the desired format.
    *
    * @param toMagic     The magic value to convert to
    * @param firstOffset The starting offset for returned records. This only impacts some cases. See
    *                    { @link  AbstractRecords#downConvert(Iterable, byte, long, Time) for an explanation.
     * @param time instance used for reporting stats
     * @return A ConvertedRecords instance which may or may not contain the same instance in its records field.
     */
  override def downConvert(toMagic: Byte, firstOffset: Long, time: Time) =
    originalRecords.downConvert(toMagic, firstOffset, time)

  /**
    * Get an iterator over the records in this log. Note that this generally requires decompression,
    * and should therefore be used with care.
    *
    * @return The record iterator
    */
  override def records() = new AccessableRecords(originalRecords.records(), recordAuthorizer).records
}
