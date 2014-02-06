package kafka.clients.producer;

/**
 * A callback interface that the user can implement to allow code to execute when the request is complete. This callback
 * will generally execute in the background I/O thread so it should be fast.
 */
public interface Callback {

    /**
     * A callback method the user can implement to provide asynchronous handling of request completion. This method will
     * be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
     * non-null.
     * @param metadata The metadata for the record that was sent (i.e. the partition and offset). Null if an error
     *        occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception);
}
