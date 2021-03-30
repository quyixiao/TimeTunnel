package com.taobao.timetunnel2.router.loadbalance;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.timetunnel2.router.common.ParamsKey;
import com.taobao.timetunnel2.router.exception.LoadBalanceException;

public class RoundRobinStatelessLoadBalancerTest extends TestCase{
	private RouterContext handler;

	@Before
	public void setUp() throws Exception {
		handler = RouterContext.getContext();
		LoadBalancer policy = LoadBalancerFactory.getLoadBalancerPolicy("RoundRobinStatelessLoadBalancer");	
		handler.setPolicy(ParamsKey.LBPolicy.policy, policy);	
	}

	@After
	public void tearDown() throws Exception {
		handler.cleanup();
	}
	
	@Test
	public void testFailOverChoose() {
		LoadBalancer policy = handler.getPolicy(ParamsKey.LBPolicy.policy);
		//host1-connect		
		try {
			String broker = policy.choose("acookie", "host1");
			Assert.assertEquals("10.232.130.1:9999", broker);
			//host2-connect
			broker = policy.choose("acookie", "host2");		
			Assert.assertEquals("10.232.130.2:9999", broker);
			//host3-connect
			broker = policy.choose("acookie", "host3");
			Assert.assertEquals("10.232.130.3:9999", broker);
			//host2-reconnect
			broker = policy.choose("acookie", "host2");
			Assert.assertEquals("10.232.130.1:9999", broker);
			//host4-connect
			broker = policy.choose("acookie", "host4");
			Assert.assertEquals("10.232.130.2:9999", broker);
			//host3-reconnect
			broker = policy.choose("acookie", "host3");
			Assert.assertEquals("10.232.130.3:9999", broker);
			//host2-third-reconnect
			broker = policy.choose("acookie", "host2");
			Assert.assertEquals("10.232.130.1:9999", broker);
			//host5-connect
			broker = policy.choose("acookie", "host5");
			Assert.assertEquals("10.232.130.2:9999", broker);
			//host2-fourth-reconnect
			broker = policy.choose("acookie", "host2");
			Assert.assertEquals("10.232.130.3:9999", broker);

		} catch (LoadBalanceException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testChoose() {
		LoadBalancer policy = handler.getPolicy(ParamsKey.LBPolicy.policy);
		Map<String, Integer> accumulator = new HashMap<String, Integer>();
		try {
			for (int i = 0; i < 1000; i++) {
				counter(accumulator, policy.choose("acookie", "host1"));
				counter(accumulator, policy.choose("acookie", "host2"));
				counter(accumulator, policy.choose("acookie", "host3"));
				counter(accumulator, policy.choose("acookie", "host2"));
				counter(accumulator, policy.choose("acookie", "host4"));
				counter(accumulator, policy.choose("acookie", "host3"));
				counter(accumulator, policy.choose("acookie", "host2"));
				counter(accumulator, policy.choose("acookie", "host5"));
				counter(accumulator, policy.choose("acookie", "host2"));
			}
			for (String key : accumulator.keySet()) {
				System.out.println("key=" + key + ",count="
						+ accumulator.get(key));
			}
		} catch (LoadBalanceException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void counter(Map<String, Integer> accumulator, String key){
		Integer value= accumulator.get(key);
		if(value!=null)			
			accumulator.put(key, value.intValue()+1);
		else
			accumulator.put(key, 1);
	}

}
