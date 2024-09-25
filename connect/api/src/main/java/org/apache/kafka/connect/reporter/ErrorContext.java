package org.apache.kafka.connect.reporter;

public class ErrorContext<T> {

	private final String stage;
	private final String executingClassName;
	private final T original;
	private final Throwable error;

	public ErrorContext(String stage, String executingClassName, T original, Throwable error) {
		this.stage = stage;
		this.executingClassName = executingClassName;
		this.original = original;
		this.error = error;
	}

	public String stage() {
		return stage;
	}

	public String executingClassName() {
		return executingClassName;
	}

	public T original() {
		return original;
	}

	public Throwable error() {
		return error;
	}

}
