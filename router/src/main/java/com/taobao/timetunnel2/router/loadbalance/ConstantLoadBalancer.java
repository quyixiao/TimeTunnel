package com.taobao.timetunnel2.router.loadbalance;

import com.taobao.timetunnel2.router.exception.LoadBalanceException;


public class ConstantLoadBalancer implements LoadBalancer {
	
	private RouterMap routerMap = RouterMap.getInstance();

	public synchronized String choose(String topic, String clientId) throws LoadBalanceException {
		String broker = routerMap.getClientConstStatus(clientId);
		if(broker == null){
			broker = routerMap.getCurrent(topic);
			routerMap.setClientConstStatus(clientId, broker);
		}
		return broker;			
	}
}
