package com.taobao.timetunnel2.router.loadbalance;

import com.taobao.timetunnel2.router.exception.LoadBalanceException;

public interface LoadBalancer {	
	String choose(String topic, String clientId) throws LoadBalanceException;
}
