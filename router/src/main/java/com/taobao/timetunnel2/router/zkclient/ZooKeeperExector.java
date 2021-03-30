package com.taobao.timetunnel2.router.zkclient;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

import com.taobao.timetunnel2.router.exception.ZKCliException;


public class ZooKeeperExector extends ZookeeperRecyclableService implements ZookeeperService{
	private static final Logger log = Logger.getLogger(ZooKeeperExector.class);
	private ZooKeeperRecyclableClient zkc = null;
	private int retryCount = 3;
	private int retryInterval = 100;
	
	public ZooKeeperExector(ZookeeperProperties zProps){
		super(zProps.getZkSrvList(), zProps.getZkTimeout());
		this.retryCount = zProps.getRetryCount();
		this.retryInterval = zProps.getRetryInterval();
		this.connect();
		this.zkc = getZooKeeperClient();
	}
	
	private boolean isAlive(){
		return zkc.getZooKeeper().getState().isAlive();
	}
	
	private boolean isConnected(){
		return zkc.getZooKeeper().getState().equals(States.CONNECTED);
	}
	
	private boolean isConnecting(){
		return zkc.getZooKeeper().getState().equals(States.CONNECTING);
	}
	
	private void checkOpen() throws ZKCliException {
		//connected
		if(isConnected()){
			return;
		}
		//disconnected
		if(!isAlive()){
			int count = retryCount;			
			while (count > 0) {
				this.reconnect();
				if (isConnected())
					return;			
				count--;
			}			
		}
		//connecting
		if(isConnecting()){
			int count = retryCount;			
			while (count > 0) {
				try {
					Thread.sleep(retryInterval);
				} catch (InterruptedException e) {
					log.error(e);
				}
				if (isConnected())
					return;
				count--;
			}				
		}				
		throw new ZKCliException("ZooKeeper connection is lost.");
	}
	
	@Override
	public String getData(String path) throws ZKCliException {
		checkOpen();
		return zkc.getPathDataAsStr(path);
	}

	@Override
	public String getData(String path, DataCallback cb, Object ctx) throws ZKCliException {
		checkOpen();
		GetDataCallback dcb = new GetDataCallback();
		CountDownLatch signal = new CountDownLatch(1);
		zkc.getZooKeeper().getData(path, false, cb, signal);
		return dcb.getResult();
	}

	@Override
	public List<String> getChildren(String path) throws ZKCliException {
		checkOpen();
		return zkc.listPathChildren(path);
	}

	@Override
	public List<String> getChildren(String path, ChildrenCallback cb, Object ctx)
			throws ZKCliException {
		checkOpen();
		CCallback ccb = new CCallback(); 
		CountDownLatch signal = new CountDownLatch(1);
		zkc.getZooKeeper().getChildren(path, false, ccb, signal);	
		return ccb.getResult();
	}

	@Override
	public void setData(String path, String value) throws ZKCliException{
		checkOpen();
		if(!zkc.existPath(path, false))
			zkc.createPathRecursively(path, CreateMode.PERSISTENT);
		zkc.setPathDataAsStr(path, value);
	}

	@Override
	public void setData(String path, String value, StatCallback cb, Object ctx) throws ZKCliException {
		checkOpen();
		if(!zkc.existPath(path, false))
			zkc.createPathRecursively(path, CreateMode.PERSISTENT);
		CountDownLatch signal = new CountDownLatch(1);
		SetDataCallBack cbk = new SetDataCallBack();
		zkc.getZooKeeper().setData(path, value.getBytes(), -1, cbk, signal);		
		try {
			signal.await(2, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			log.error(e);
		}
	}

	@Override
	public void delete(String path, boolean cascade) throws ZKCliException {
		checkOpen();
		if(cascade)
			zkc.deletePathTree(path);
		else
			zkc.deletePath(path);
	}

	@Override
	public void delete(String path, boolean cascade, VoidCallback cb,
			Object ctx) throws ZKCliException {
		checkOpen();
		VVoidCallback vcb = new VVoidCallback();
		CountDownLatch signal = new CountDownLatch(1);
		zkc.getZooKeeper().delete(path, -1, vcb, signal);		
	}

	@Override
	public void close() throws ZKCliException {
		checkOpen();
		zkc.close();	
	}
	
	class VVoidCallback implements VoidCallback{
		@Override
		public void processResult(int rc, String path, Object ctx) {
			CountDownLatch signal = (CountDownLatch)ctx;
			Code code = Code.get(rc);
			if (code.equals(Code.SESSIONEXPIRED)) {
				reconnect();	
			}
			signal.countDown();			
		}		
	}
	
	class GetDataCallback implements DataCallback{
		private String data;
		public String getResult(){
			return data;
		}
		@Override
		public void processResult(int rc, String path, Object ctx,
				byte[] data, Stat stat) {
			CountDownLatch signal = (CountDownLatch)ctx;
			Code code = Code.get(rc);
			if (code.equals(Code.OK)) {
				if (data!=null)
					this.data = new String(data);
			}else if (code.equals(Code.SESSIONEXPIRED)) {
				reconnect();	
			}
			signal.countDown();
		}		
	}

	class SetDataCallBack implements StatCallback{
		@Override
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			CountDownLatch signal = (CountDownLatch)ctx;
			Code code = Code.get(rc);	
			if (code.equals(Code.SESSIONEXPIRED)) {
				reconnect();
			}
			signal.countDown();
		}		
	}
	
	class CCallback implements ChildrenCallback{
		private List<String> dirs; 		
		@Override
		public void processResult(int rc, String path, Object ctx,
				List<String> dirs) {
			CountDownLatch count = (CountDownLatch)ctx;
			Code code = Code.get(rc);
			if (code.equals(Code.OK)) {				
				this.dirs = dirs;
			} else if (code.equals(Code.NONODE)) {
				this.dirs = null;
			} else if (code.equals(Code.SESSIONEXPIRED) || code.equals(Code.CONNECTIONLOSS)) {
				reconnect();
			} else if (code.equals(Code.NOAUTH)) {
				return;
			} 		
			count.countDown();
		}	
		
		public List<String> getResult(){
			return dirs;
		}
	}
	
}
