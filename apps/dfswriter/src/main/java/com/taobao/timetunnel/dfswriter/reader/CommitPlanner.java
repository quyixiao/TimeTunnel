package com.taobao.timetunnel.dfswriter.reader;

import java.io.File;

import com.taobao.timetunnel.client.Message;

public interface CommitPlanner {
	public MultiPassMode getMultiPassMode(File f);

	public String getGroup(File f, String baseDir);

	public String getSecondPassGroup(MultiPassMode multiPassMode);

	public boolean isInNewerCommitIntervalThan(File f1, File f2);

	public boolean checkMessage(Message m, MultiPassMode multiPassMode, int pass);

	public String getBackupFilePath(File f, String baseDir, String backupDir);
}
