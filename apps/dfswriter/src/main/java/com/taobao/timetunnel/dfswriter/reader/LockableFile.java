package com.taobao.timetunnel.dfswriter.reader;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import org.apache.log4j.Logger;

public class LockableFile {
	 private static final Logger log = Logger.getLogger(LockableFile.class);
 	 private File file;
  	 private RandomAccessFile raf;
  	 private FileChannel channel;
  	 private FileLock lock;
  	 
  	 public LockableFile(File file) throws Exception{
  		 this.file=file;
  		 this.raf=new RandomAccessFile(file,"rw");
		 this.channel=raf.getChannel();
  	 }
  	 public void lock() throws Exception {
  		 lock=channel.tryLock();
  		 if (lock==null || !file.exists() || file.length()==0)
  			 throw new java.nio.channels.OverlappingFileLockException();
  		 log.info("("+System.getProperty("pid")+") Lock file "+file.getPath()+", exist "+file.exists()+", file size "+file.length());
  	 }
  	 public File getFile() {
  		 return file;
  	 }
  	 public RandomAccessFile getRaf() {
  		 return raf;
  	 }
  	 public FileChannel getChannel() {
  		 return channel;
  	 }
  	 public FileLock getLock() {
  		 return lock;
  	 }
  	 public void close() {
  		 if (lock!=null)
				try { lock.release(); }catch (Exception ignored){ignored.printStackTrace();}
	     if (channel!=null)
				try { channel.close(); } catch (Exception ignored){ignored.printStackTrace();}
		 if (raf!=null)
				try  { raf.close();  } catch (Exception ignored) {ignored.printStackTrace();}
  	 }

}
  