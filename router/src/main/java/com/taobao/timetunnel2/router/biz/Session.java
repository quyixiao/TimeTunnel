package com.taobao.timetunnel2.router.biz;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

public class Session{

	private String type;
	private String timeout;
	private String subscriber;
	private String receiveWindowSize;
	
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getTimeout() {
		return timeout;
	}
	public void setTimeout(String timeout) {
		this.timeout = timeout;
	}
	public String getSubscriber() {
		return subscriber;
	}
	public void setSubscriber(String subscriber) {
		this.subscriber = subscriber;
	}
	public String getReceiveWindowSize() {
		return receiveWindowSize;
	}
	public void setReceiveWindowSize(String receiveWindowSize) {
		this.receiveWindowSize = receiveWindowSize;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof Session))
			return false;

		Session o = (Session) obj;

		return new EqualsBuilder().append(this.type, o.type)
				.append(this.timeout, o.timeout)
				.append(this.subscriber, o.subscriber)
				.append(this.receiveWindowSize, o.receiveWindowSize).isEquals();
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder(17, 37).append(type).append(timeout)
				.append(subscriber).append(receiveWindowSize)
				.toHashCode();
	}
}
