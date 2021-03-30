package com.taobao.timetunnel.dfswriter.util;

import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;

public class HDFSWriter {
	public static void main(String args[]) {
		HDFSWriter writer = new HDFSWriter("hdfs://xxxx", "1");
		SequenceFile.Writer f = writer.open("/user/test123");
		writer.write(f, "abc".getBytes());
		writer.close(f);
	}

	private Configuration conf;
	private FileSystem fs;
	private static final Logger log = Logger.getLogger(HDFSWriter.class);

	public HDFSWriter(String url, String numOfReplicas) {
		conf = new Configuration();
		conf.set("fs.default.name", url);
		conf.set("dfs.replication", numOfReplicas);
		// conf.set("hadoop.job.ugi","xxxx,xxxxx");
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public SequenceFile.Writer open(String path) {
		try {
			log.info("DFS create " + path);
			return SequenceFile.createWriter(fs, conf, new Path(path), LongWritable.class, Text.class, SequenceFile.CompressionType.BLOCK);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void write(SequenceFile.Writer writer, byte[] data) {
		try {
			String strData = new String(data, "UTF-8");
			Text text = new Text();
			for (String line : strData.split("\n")) {
				text.set(line);
				writer.append(ZERO, text);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void close(SequenceFile.Writer writer) {
		try {
			log.info("DFS closing file");
			writer.close();
			log.info("DFS closing file done");
		} catch (IOException e) {
			log.error("Error closing DFS file");
			throw new RuntimeException(e);
		}
	}

	public long getCurrentSize(SequenceFile.Writer writer) {
		try {
			return writer.getLength();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void rename(String src, String dest) {
		try {
			boolean success = fs.rename(new Path(src), new Path(dest));
			if (!success)
				throw new RuntimeException("Fail to rename " + src + " to " + dest);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static final LongWritable ZERO = new LongWritable(0);
}
