package kafka.clients.producer;

/**
 * A callback interface that the user can implement to allow code to execute when the request is complete. This callback
 * will execute in the background I/O thread so it should be fast.
 */
public interface Callback {

    /**
     * A callback method the user should implement. This method will be called when the send to the server has
     * completed.
     * @param send The results of the call. This send is guaranteed to be completed so none of its methods will block.
     */
    public void onCompletion(RecordSend send);
}
