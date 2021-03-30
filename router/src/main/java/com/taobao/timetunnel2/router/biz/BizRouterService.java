package com.taobao.timetunnel2.router.biz;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.taobao.timetunnel.thrift.router.Constants;
import com.taobao.timetunnel.thrift.router.ExReason;
import com.taobao.timetunnel.thrift.router.RouterException;
import com.taobao.timetunnel.thrift.router.RouterService;
import com.taobao.timetunnel2.router.common.ParamsKey;
import com.taobao.timetunnel2.router.common.RouterConsts;
import com.taobao.timetunnel2.router.common.Util;
import com.taobao.timetunnel2.router.exception.LoadBalanceException;
import com.taobao.timetunnel2.router.exception.ServiceException;
import com.taobao.timetunnel2.router.loadbalance.LoadBalancer;
import com.taobao.timetunnel2.router.loadbalance.RouterContext;

public class BizRouterService implements RouterService.Iface {
	private static final Logger log =Logger.getLogger(BizRouterService.class);
	private RouterContext routercontext;
	
	public BizRouterService(RouterContext routercontext){
		this.routercontext = routercontext;
	}
	
	@Override
	public String getBroker(String user, String pwd, String topic,
			String apply, Map<String, String> prop)
			throws RouterException,	TException {
		String clientId = prop.get(Constants.LOCAL_HOST)+RouterConsts.ID_SPLIT+topic;
		String type = prop.get(Constants.TYPE);
		log.info(String.format(
				"One request has been received: thread=%s,user=%s,pwd=%s,topic=%s,isApply=%s,clientId=%s,type=%s,prop=%s",
				Thread.currentThread().getId(),
				user, pwd, topic, apply, clientId, type, prop));		
		LoadBalancer lb = null;
		if(RouterConsts.LB_APPLY.equalsIgnoreCase(apply))
			lb = routercontext.getPolicy(ParamsKey.LBPolicy.s_policy);
		else
			lb = routercontext.getPolicy(ParamsKey.LBPolicy.policy);
		String sessionId = null;
		try{
			if ((sessionId = routercontext.authenticate(user, pwd, topic, prop)) != null) {
				List<String> presrvlist = routercontext.getSessionStats(topic);
				if (presrvlist != null && presrvlist.size() > 0) {
					BrokerSrvRlt rslt = new BrokerSrvRlt();
					List<String> serverList = new ArrayList<String>();
					rslt.setSessionId(sessionId);
					if ("PUB".equalsIgnoreCase(type)) {
						String chosensrv = null;
						try {
							chosensrv = lb.choose(topic, clientId);
						} catch (LoadBalanceException e) {
							ExReason ec = ExReason.NOTFOUND_BROKERURL;	
							log.warn("The request has been resolved."+ec.name()+"["+ec.getValue()+"],"+e.getMessage());										
							throw new RouterException((short)ec.getValue(),
									ec.name(), e.getMessage());
						}
						serverList.add(chosensrv);
						log.info(String.format(
								"One request has been received: thread=%s,sessionId=%s,brokerurl=%s",
								Thread.currentThread().getId(), sessionId, chosensrv));						
					} else {
						for (String presrv : presrvlist) {
							serverList.add(presrv);
						}
					}
					rslt.setBrokerserver(serverList);
					log.debug("The request has been resolved.");
					return Util.toJsonStr(rslt);
				} else {
					ExReason ec = ExReason.NOTFOUND_BROKERURL;
					log.warn("The request has been resolved."+ec.name()+"["+ec.getValue()+"],"+RouterConsts.ERRMSG_NO_SERVER);	
					throw new RouterException((short)ec.getValue(),
							ec.name(), RouterConsts.ERRMSG_NO_SERVER);
				}

			}
			ExReason ec = ExReason.INVALID_USERORPWD;
			log.warn("The request has been resolved."+ec.name()+"["+ec.getValue()+"],"+RouterConsts.ERRMSG_AUTH_FAIL);
			throw new RouterException((short)ec.getValue(),
					ec.name(), RouterConsts.ERRMSG_AUTH_FAIL);
		}catch(ServiceException e){
			ExReason ec = ExReason.SERVICE_UNAVAILABLE;
			log.warn("The request has been resolved."+ec.name()+"["+ec.getValue()+"],"+RouterConsts.ERRMSG_UNAVAILABLE);
			throw new RouterException((short)ec.getValue(),
					ec.name(), RouterConsts.ERRMSG_UNAVAILABLE+":"+e.getMessage());
		}
	}

}
