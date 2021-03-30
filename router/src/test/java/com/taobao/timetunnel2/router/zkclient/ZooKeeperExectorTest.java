package com.taobao.timetunnel2.router.zkclient;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import junit.framework.TestCase;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.taobao.timetunnel2.router.common.ParamsKey;
import com.taobao.timetunnel2.router.common.RouterConsts;
import com.taobao.timetunnel2.router.exception.ZKCliException;

public class ZooKeeperExectorTest extends TestCase{
	private ZookeeperService zks;
	@Before
	public void setUp() throws Exception {
		Properties prop = new Properties();		
		System.out.println(this.getClass().getClassLoader().getResourceAsStream(RouterConsts.ROUTER_PATH));
		prop.load(this.getClass().getClassLoader().getResourceAsStream(RouterConsts.ROUTER_PATH));
		ZookeeperProperties zprops = new ZookeeperProperties(prop);
		zks = new ZooKeeperExector(zprops);
	}
	
	@After
	public void tearDown() throws Exception {
		zks.close();
	}
	
	@Test
	public void testAsyncGetData() {
		/*DCallback dcb = new DCallback();
		CountDownLatch count = new CountDownLatch(1);*/
		try {
			String data = zks.getData("/categories/acookie", null, null);
			System.out.println("testGetData [/categories/acookie] result="+data);
		} catch (ZKCliException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testAsyncGetChildren() {
		try { 
			List<String> dirs = zks.getChildren("/clients/host2:8080-acookie", null, null);
			if(dirs!=null){
				for (String d : dirs) {
					VCallback vcb = new VCallback();
					zks.delete("/clients/host2:8080-acookie/" + d, false, vcb,
							null);
				}
			}
		} catch (ZKCliException e) {
			e.printStackTrace();
		} 		
	}

	@Test
	public void testAsyncDelete() {		
		try {
			zks.delete("/clients/host1:8080-acookie/646d90b39f1d0774104863882f1f5c81", false, null, null);
		} catch (ZKCliException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	class VCallback implements VoidCallback{

		@Override
		public void processResult(int rc, String arg1, Object ctx) {
			System.out.println(rc+",deleted.");			
		}
		
	}
	
	class CCallback implements ChildrenCallback{
		private List<String> dirs; 
		
		@Override
		public void processResult(int rc, String path, Object ctx,
				List<String> dirs) {			
			this.dirs = dirs;
			CountDownLatch count = (CountDownLatch)ctx;
			count.countDown();
		}	
		
		public List<String> getResult(){
			return dirs;
		}

	}
	
	@Test
	public void testDelete() throws NoNodeException {		
		try {
			List<String> brokers = zks.getChildren(ParamsKey.ZNode.broker);
			if(brokers!=null){	
				for(String group: brokers){
					List<String> nodes = zks.getChildren(ParamsKey.ZNode.broker+"/"+group);
					if(nodes!=null){
						for(String node: nodes){
							zks.delete(ParamsKey.ZNode.broker+"/"+group+"/"+ node,true);	
						}
					}
					zks.delete(ParamsKey.ZNode.broker+"/"+group,true);	
				}
			}
		} catch (ZKCliException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}	
	
	@Test
	public void testDeleteClients() throws NoNodeException {
		try {
			List<String> sessions = zks.getChildren(ParamsKey.ZNode.session);
			if(sessions!=null){	
				for(String clientId: sessions){
					System.out.println("clientId="+clientId);
					if(!(clientId.equals("host1:8080-click") || clientId.equals("host1:8080-acookie"))){
					List<String> nodes = zks.getChildren(ParamsKey.ZNode.session+"/"+clientId);
					if(nodes!=null){
						for(String node: nodes){
							
							zks.delete(ParamsKey.ZNode.broker+"/"+clientId+"/"+ node,true);	
						}
					}
					zks.delete(ParamsKey.ZNode.session+"/"+clientId,true);	
					}
				}
			}
		} catch (ZKCliException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	@Test
	public void testSetData() {
		try {
			zks.setData(ParamsKey.ZNode.user+"/tt", "{\"password\":\"3\"}");
			Assert.assertEquals("{\"password\":\"3\"}", zks.getData(ParamsKey.ZNode.user+"/tt"));
			zks.setData(ParamsKey.ZNode.user+"/tt", "{\"password\":\"2\"}");
			Assert.assertEquals("{\"password\":\"2\"}", zks.getData(ParamsKey.ZNode.user+"/tt"));
			zks.setData(ParamsKey.ZNode.broker+"/group1/b0000000000", "{\"external\":9999,\"internal\":9998,\"host\":\"10.232.130.1\"}");
			Assert.assertEquals("{\"external\":9999,\"internal\":9998,\"host\":\"10.232.130.1\"}", zks.getData(ParamsKey.ZNode.broker+"/group1/b0000000000"));
			zks.setData(ParamsKey.ZNode.broker+"/group1/b0000000001", "{\"external\":9999,\"internal\":9998,\"host\":\"10.232.130.2\"}");
			Assert.assertEquals("{\"external\":9999,\"internal\":9998,\"host\":\"10.232.130.2\"}", zks.getData(ParamsKey.ZNode.broker+"/group1/b0000000001"));
			zks.setData(ParamsKey.ZNode.broker+"/group1/b0000000002", "{\"external\":9999,\"internal\":9998,\"host\":\"10.232.130.3\"}");
			Assert.assertEquals("{\"external\":9999,\"internal\":9998,\"host\":\"10.232.130.3\"}", zks.getData(ParamsKey.ZNode.broker+"/group1/b0000000002"));

		} catch (ZKCliException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void testGetChildren() {
		try {
			List<String> topics = zks.getChildren(ParamsKey.ZNode.topic);
			for(String topic: topics){
				System.out.println(topic);
			}			
		} catch (ZKCliException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testGetData() {
		try {
			String pwd = zks.getData(ParamsKey.ZNode.user+"/"+"tt");
			Assert.assertEquals("{\"password\":\"2\"}", pwd);
		} catch (ZKCliException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}

	
	
	class DCallback implements DataCallback{
		private byte[] data; 
		
		@Override
		public void processResult(int rc, String path, Object ctx,
				byte[] data, Stat stat) {			
			System.out.println(rc+"******,"+stat.toString()+"**********"+new String(data)+"*********");	
			this.data = data;
			CountDownLatch count = (CountDownLatch)ctx;
			count.countDown();
		}
		
		public byte[] getResult(){
			return data;
		}
	}

}
