module Kafka
  module IO
    attr_accessor :socket, :host, :port

    def connect(host, port)
      raise ArgumentError, "No host or port specified" unless host && port
      self.host = host
      self.port = port
      self.socket = TCPSocket.new(host, port)
    end

    def reconnect
      self.disconnect
      self.socket = self.connect(self.host, self.port)
    end

    def disconnect
      self.socket.close rescue nil
      self.socket = nil
    end

    def write(data)
      self.reconnect unless self.socket
      self.socket.write(data)
    rescue Errno::ECONNRESET, Errno::EPIPE, Errno::ECONNABORTED
      self.reconnect
      self.socket.write(data) # retry
    end

    def read(length)
      begin
        self.socket.read(length)
      rescue Errno::EAGAIN
        self.disconnect
        raise Errno::EAGAIN, "Timeout reading from the socket"
      end
    end
  end
end
