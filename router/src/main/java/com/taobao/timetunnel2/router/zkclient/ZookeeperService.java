package com.taobao.timetunnel2.router.zkclient;

import java.util.List;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;

import com.taobao.timetunnel2.router.exception.ZKCliException;

public interface ZookeeperService {

	public String getData(String path) throws ZKCliException;
	
	public String getData(String path, DataCallback cb, Object ctx) throws ZKCliException;

	public List<String> getChildren(String path) throws ZKCliException;
	
	public List<String> getChildren(String path, ChildrenCallback cb, Object ctx) throws ZKCliException;

	public void setData(String path, String value) throws ZKCliException;
	
	public void setData(String path, String value, StatCallback cb, Object ctx) throws ZKCliException;
	
	public void delete(String path, boolean cascade) throws ZKCliException;	

	public void delete(String path, boolean cascade, VoidCallback cb, Object ctx) throws ZKCliException;	
	
	public void close() throws ZKCliException;
}
