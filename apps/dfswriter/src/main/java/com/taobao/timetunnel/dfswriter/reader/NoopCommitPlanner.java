package com.taobao.timetunnel.dfswriter.reader;

import java.io.File;

import com.taobao.timetunnel.client.Message;

public class NoopCommitPlanner implements CommitPlanner {

	public static void main(String args[]) {
		new NoopCommitPlanner().getGroup(new File("c://"), "/home/tucuicui/test/savefile");
	}

	private String baseDir = null;

	@Override
	public boolean checkMessage(Message m, MultiPassMode multiPassMode, int pass) {
		return true;
	}

	@Override
	public String getGroup(File f, String dir) {
		if (baseDir == null)
			baseDir = dir;
		String path = f.getPath();
		int startIndex = path.indexOf(baseDir);
		if (startIndex == -1)
			throw new RuntimeException("Oops file " + f.getPath() + " is not in the base dir " + baseDir);
		startIndex += baseDir.length();
		int lastIndex = path.lastIndexOf('/');
		String group = "_default";
		if (startIndex < lastIndex)
			group = path.substring(startIndex, lastIndex);
		return group;
	}

	@Override
	public MultiPassMode getMultiPassMode(File f) {
		return new MultiPassMode(MultiPassMode.Mode.OFF, null);
	}

	@Override
	public String getSecondPassGroup(MultiPassMode multiPassMode) {
		return "";
	}

	@Override
	public boolean isInNewerCommitIntervalThan(File f1, File f2) {
		return !getGroup(f1, baseDir).equals(getGroup(f2, baseDir));
	}

	@Override
	public String getBackupFilePath(File f, String baseDir, String backupDir) {
		String path = f.getPath();
		return path.replaceFirst(baseDir, backupDir);
	}
}
