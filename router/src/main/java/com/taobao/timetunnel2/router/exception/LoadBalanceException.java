package com.taobao.timetunnel2.router.exception;

public class LoadBalanceException extends Exception {	

	private static final long serialVersionUID = 2012192565070851507L;

	public LoadBalanceException() {
		super();
	}

	public LoadBalanceException(String message, Throwable cause) {
		super(message, cause);
	}

	public LoadBalanceException(String message) {
		super(message);
	}

	public LoadBalanceException(Throwable cause) {
		super(cause);
	}

}
