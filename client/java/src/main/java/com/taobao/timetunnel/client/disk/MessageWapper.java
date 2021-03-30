package com.taobao.timetunnel.client.disk;

import java.nio.ByteBuffer;

/**
 * 
 * @author <jiugao@taobao.com>
 * @created 2010-10-14
 * 
 */
public class MessageWapper {
	private final ByteBuffer bf;
	private final boolean firstMInFile;
	private final long startPos;
	private final String currentFileName;

	public MessageWapper(ContentBuffer content, boolean firstMInFile, long startPos, String currentFileName) {
		super();
		this.bf = content.get();
		this.firstMInFile = firstMInFile;
		this.startPos = startPos;
		this.currentFileName = currentFileName;
	}

	public ByteBuffer getBf() {
		return bf;
	}

	public boolean isFirstMInFile() {
		return firstMInFile;
	}

	public long getStartPos() {
		return startPos;
	}

	public String getCurrentFileName() {
		return currentFileName;
	}

}
