package com.taobao.timetunnel2.router.loadbalance;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.timetunnel2.router.common.ParamsKey;
import com.taobao.timetunnel2.router.exception.LoadBalanceException;

public class LoadBalancerFactoryTest extends TestCase {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {

	}

	@Test
	public void testGetLoadBalancerPolicy() {
		LoadBalancer policy1 = LoadBalancerFactory.getLoadBalancerPolicy("ConstantLoadBalancer");
		Assert.assertEquals("com.taobao.timetunnel2.router.loadbalance.ConstantLoadBalancer", policy1.getClass().getName());
		LoadBalancer policy2 = LoadBalancerFactory.getLoadBalancerPolicy("RoundRobinStatelessLoadBalancer");
		Assert.assertEquals("com.taobao.timetunnel2.router.loadbalance.RoundRobinStatelessLoadBalancer", policy2.getClass().getName());
	}

}
