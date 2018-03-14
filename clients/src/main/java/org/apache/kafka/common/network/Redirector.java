package org.apache.kafka.common.network;

import java.net.InetSocketAddress;

import org.apache.kafka.common.Configurable;

public interface Redirector extends Configurable {

	public InetSocketAddress redirect(InetSocketAddress address);
	
}
