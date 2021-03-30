package com.taobao.timetunnel.dfswriter.reader;
import com.taobao.timetunnel.client.Message;

public interface RecordHandler {
	
    void setGroup(String group);

	long handleRecord(Message message);
		
	void commit(MultiPassMode multiPassMode,int pass);
}
