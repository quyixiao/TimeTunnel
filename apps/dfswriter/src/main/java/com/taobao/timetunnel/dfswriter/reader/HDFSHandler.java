package com.taobao.timetunnel.dfswriter.reader;
import java.util.UUID;

import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Logger;

import com.taobao.timetunnel.client.Message;
import com.taobao.timetunnel.dfswriter.util.HDFSWriter;
import com.taobao.timetunnel.dfswriter.util.NetUtil;

public class HDFSHandler implements RecordHandler {
	 private static final Logger log = Logger.getLogger(HDFSHandler.class);
	 private SequenceFileStruct seqFile=null;
	 private HDFSWriter hdfsWriter;
     private String tag;
     private String baseDir;
     private String group;
     private String hostName;
     
     private String pendingSeqFilePath=null;
     
	public HDFSHandler(String tag,String hdfsUrl,String baseDir,String numOfReplicas) {
		 this.tag=tag;
	     hdfsWriter=new HDFSWriter(hdfsUrl,numOfReplicas);
	     this.baseDir=baseDir;
	     this.hostName=NetUtil.getLocalHostName();
	 }
	
	 public void setGroup(String group) {
		 log.info("("+System.getProperty("pid")+") set group "+group);
		 this.group=group;
	 }
	 
	 public long handleRecord(Message message){
		 try {
			 if (seqFile==null) {
				 seqFile=new SequenceFileStruct();
				 seqFile.path=baseDir+"/"+tag+"/"+group+"/"+hostName+"/"+System.getProperty("pid")+"_"+UUID.randomUUID().toString()+".log.tmp";
				 log.info("("+System.getProperty("pid")+")DFS OPEN => "+seqFile.path);
				 seqFile.writer=hdfsWriter.open(seqFile.path);
			 }
			 if (message!=null) {
				 if (message.isCompressed())
					 message.decompress();
				 hdfsWriter.write(seqFile.writer, message.getContent());
			 }
			 return hdfsWriter.getCurrentSize(seqFile.writer);
		 }
		 catch (Exception e) {
			log.error("Exception occurred while writing to HDFS, program aborted",e);
			System.exit(-1);
			return 0L;
		 }
     }
	
	 public void commit(MultiPassMode multiPassMode,int pass) {
		 try {
		 log.info("("+System.getProperty("pid")+")commit, multi pass "+multiPassMode+", pass "+pass);
		 boolean deferredCommit=false;
		 if (seqFile!=null) {
			 log.info("("+System.getProperty("pid")+") close DFS =>"+seqFile.path);
			 hdfsWriter.close(seqFile.writer);
    		 if (multiPassMode.isMultiPassEnabled() && pass==0) {
    			 pendingSeqFilePath=seqFile.path;
    			 deferredCommit=true;
    		 }
    		 else  
    			 hdfsWriter.rename(seqFile.path, seqFile.path.substring(0,seqFile.path.length()-4));
    		 seqFile=null;
    	 }
		 if (!deferredCommit && pendingSeqFilePath!=null) {
			 hdfsWriter.rename(pendingSeqFilePath,pendingSeqFilePath.substring(0,pendingSeqFilePath.length()-4));
		     pendingSeqFilePath=null;
		 }
		 }
		 catch (Exception e) {
			 log.error("Exception occured while commiting to HDFS, program aborted",e);
			 System.exit(-2);
		 }
	 }
   
	 private static class SequenceFileStruct {
 		String path;
     	SequenceFile.Writer writer;
     }
}
