package org.apache.kafka.clients.consumer;

public class SubscriptionPattern {
	final private String pattern;

	public SubscriptionPattern(final String pattern) {
		this.pattern = pattern;
	}

	public String pattern() {
		return this.pattern;
	}
}
