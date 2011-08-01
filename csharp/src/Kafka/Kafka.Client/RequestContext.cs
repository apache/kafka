using System.Net.Sockets;

namespace Kafka.Client
{
    /// <summary>
    /// The context of a request made to Kafka.
    /// </summary>
    /// <typeparam name="T">
    /// Must be of type <see cref="AbstractRequest"/> and represents the type of request
    /// sent to Kafka.
    /// </typeparam>
    public class RequestContext<T> where T : AbstractRequest
    {
        /// <summary>
        /// Initializes a new instance of the RequestContext class.
        /// </summary>
        /// <param name="networkStream">The network stream that sent the message.</param>
        /// <param name="request">The request sent over the stream.</param>
        public RequestContext(NetworkStream networkStream, T request)
        {
            NetworkStream = networkStream;
            Request = request;
        }

        /// <summary>
        /// Gets the <see cref="NetworkStream"/> instance of the request.
        /// </summary>
        public NetworkStream NetworkStream { get; private set; }

        /// <summary>
        /// Gets the <see cref="FetchRequest"/> or <see cref="ProducerRequest"/> object
        /// associated with the <see cref="RequestContext"/>.
        /// </summary>
        public T Request { get; private set; }
    }
}
