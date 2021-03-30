package com.taobao.timetunnel2.router.zkclient;

import java.util.Properties;

import org.apache.log4j.Logger;

import com.taobao.timetunnel2.router.common.ParamsKey;
import com.taobao.timetunnel2.router.common.Util;
import com.taobao.timetunnel2.router.common.ValidationException;

public class ZookeeperProperties{
	private static final Logger log = Logger.getLogger(ZookeeperProperties.class);
	private static final long serialVersionUID = 1L;
	
	private int zkTimeout;
	private String zkSrvList;
	private int poolSize;
	private int retryCount;
	private int retryInterval;
	
	public ZookeeperProperties(Properties prop){
		if(prop!=null){			
			try {
				zkSrvList = Util.getStrParam(ParamsKey.ZKService.hosts, 
						prop.getProperty(ParamsKey.ZKService.hosts), Util.getHostName(), true);
				zkTimeout = Util.getIntParam(ParamsKey.ZKService.timeout, 
						prop.getProperty(ParamsKey.ZKService.timeout), 3000, 3000, 500000);
				poolSize = Util.getIntParam(ParamsKey.ZKClient.size, 
						prop.getProperty(ParamsKey.ZKClient.size), 1, 1, 500);
				retryCount = Util.getIntParam(ParamsKey.ZKClient.retrycount, 
						prop.getProperty(ParamsKey.ZKClient.retrycount), 3, 0, 10000);
				retryInterval = Util.getIntParam(ParamsKey.ZKClient.interval, 
						prop.getProperty(ParamsKey.ZKClient.interval), 1000, 10, 10000);
			} catch (ValidationException e) {
				log.error(e.getMessage());
				System.exit(-1);
			} catch (Exception e){
				log.error(e);
				System.exit(-1);				
			} 
		}
	}
	
	public int getZkTimeout() {
		return zkTimeout;
	}
	public void setZkTimeout(int zkTimeout) {
		this.zkTimeout = zkTimeout;
	}
	public String getZkSrvList() {
		return zkSrvList;
	}
	public void setZkSrvList(String zkSrvList) {
		this.zkSrvList = zkSrvList;
	}
	public int getPoolSize() {
		return poolSize;
	}

	public int getRetryCount() {
		return retryCount;
	}

	public void setRetryCount(int retryCount) {
		this.retryCount = retryCount;
	}

	public int getRetryInterval() {
		return retryInterval;
	}

	public void setRetryInterval(int retryInterval) {
		this.retryInterval = retryInterval;
	}
	
}
