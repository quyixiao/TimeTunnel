package com.taobao.timetunnel.dfswriter.reader;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.taobao.timetunnel.client.Message;
import com.taobao.timetunnel.client.message.MessageFactory;
import com.taobao.timetunnel.dfswriter.util.FileUtil;

public class FixModeFileReader {

	private static final Logger log = Logger.getLogger(FixModeFileReader.class);
	private String tag;
	private long maxCommitSize;
	private long autoCommitTimeout;
	private String baseDir;
	private String backupDir;
	private RecordHandler recordHandler;
	private CommitPlanner commitPlanner;
	private String dateStr;

	private List<LockableFile> processedFiles = new ArrayList<LockableFile>();
	private long currentProcessedSize = 0;
	private long lastFileProcessedTime = 0;
	private String fileNameRegx;

	public FixModeFileReader(String tag, long maxCommitSize, long autoCommitTimeout, CommitPlanner commitPlanner, String baseDir, String backupDir, RecordHandler handler, String dateStr,
			String fileNameRegx) {
		this.tag = tag;
		this.maxCommitSize = maxCommitSize;
		this.autoCommitTimeout = autoCommitTimeout;
		this.baseDir = baseDir;
		this.backupDir = backupDir;
		this.recordHandler = handler;
		this.commitPlanner = commitPlanner;
		this.dateStr = dateStr;
		this.fileNameRegx = fileNameRegx;
	}

	public void execute() {
		log.info("max commit size " + this.maxCommitSize);
		MultiPassMode multiPassMode = null;
		lastFileProcessedTime = System.currentTimeMillis();

		for (File f : FileUtil.listDirectory(getFilePath(baseDir, tag), dateStr, fileNameRegx)) {
			LockableFile file = null;
			log.debug("processing...." + f.getAbsolutePath());
			try {
				if (isCommitRequired(f)) {
					recordHandler.commit(multiPassMode, 0);
					onCommit(multiPassMode);
				}
				if (isAlreadyOpened(f))
					continue;
				file = new LockableFile(f);
				file.lock();
				multiPassMode = commitPlanner.getMultiPassMode(f);
				recordHandler.setGroup(commitPlanner.getGroup(f, getFilePath(baseDir, tag)));
				doProcessFile(file, multiPassMode, 0);
				processedFiles.add(file);
				lastFileProcessedTime = System.currentTimeMillis();
			} catch (OverlappingFileLockException e) {
				if (file != null) {
					if (file.getFile().length() == 0) {
						log.error("Delete garbage file " + file.getFile().getPath());
						file.getFile().delete();
					}
					file.close();
				}
			} catch (Exception e) {
				log.error("Exception happens while processing file " + file.getFile().getPath() + ", ignored", e);
				throw new RuntimeException(e);
			}
		}
		recordHandler.commit(multiPassMode, 0);
		try {
			onCommit(multiPassMode);
		} catch (IOException e) {
			log.error(e);
			throw new RuntimeException(e);
		}
	}

	private boolean isCommitRequired(File f) {
		if (processedFiles.size() == 0)
			return false;
		else {
			boolean b = commitPlanner.isInNewerCommitIntervalThan(f, processedFiles.get(0).getFile()) || currentProcessedSize > maxCommitSize
					|| System.currentTimeMillis() - lastFileProcessedTime > autoCommitTimeout;
			if (b) {
				log.info("(" + System.getProperty("pid") + ") CHECK commitPlanner.isDifferentCommitInterval(processedFiles.get(0).getFile(),f)"
						+ commitPlanner.isInNewerCommitIntervalThan(f, processedFiles.get(0).getFile()));
				log.info("(" + System.getProperty("pid") + ") f1 " + processedFiles.get(0).getFile().getName() + " f2 " + f.getName());
				log.info("(" + System.getProperty("pid") + ") CHECK SIZE currentProcessedSize>maxCommitSize " + currentProcessedSize + " " + maxCommitSize);
				long l = System.currentTimeMillis();
				log.info("(" + System.getProperty("pid") + ") CHECK TIMEOUT " + (l - lastFileProcessedTime) + " " + autoCommitTimeout);
			}
			return b;
		}
	}

	private boolean isAlreadyOpened(File f) {
		for (LockableFile file : processedFiles)
			if (file.getFile().getPath().equals(f.getPath()))
				return true;
		return false;
	}

	private void doProcessFile(LockableFile file, MultiPassMode multiPassMode, int pass) {
		try {
			processFile(file, multiPassMode, pass);
		} catch (Exception e) {
			log.error("Exception occured while processing " + file.getFile().getPath() + ", ignored", e);
		}
	}

	private void processFile(LockableFile file, MultiPassMode multiPassMode, int pass) throws IOException {
		log.info("(" + System.getProperty("pid") + ") process " + file.getFile().getPath());
		if (file.getFile().length() == 1) {
			recordHandler.handleRecord(null);
			return;
		}
		MappedByteBuffer byteBuffer = file.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, file.getChannel().size());
		while (byteBuffer.hasRemaining()) {
			int size = byteBuffer.getInt();
			byte[] payload = new byte[size];
			byteBuffer.get(payload);
			Message message = MessageFactory.getInstance().createMessageFrom(payload);
			Long actualBytesWritten = null;
			if (commitPlanner.checkMessage(message, multiPassMode, pass))
				actualBytesWritten = recordHandler.handleRecord(message);
			if ((!multiPassMode.isMultiPassEnabled() || pass == 0) && actualBytesWritten != null)
				currentProcessedSize = actualBytesWritten;
		}
	}

	private void onCommit(MultiPassMode multiPassMode) throws IOException {
		if (processedFiles.size() == 0)
			return;
		recycleProcessedFiles();
	}

	private void recycleProcessedFiles() {
		for (LockableFile file : processedFiles) {
			log.info("(" + System.getProperty("pid") + ")rename " + file.getFile().getPath() + " to "
					+ commitPlanner.getBackupFilePath(file.getFile(), getFilePath(baseDir, tag), getBackupFilePath(backupDir, tag)));
			// Linux only, rename while still being locked
			if (!FileUtil.rename(file.getFile(), new File(commitPlanner.getBackupFilePath(file.getFile(), getFilePath(baseDir, tag), getBackupFilePath(backupDir, tag)))))
				throw new RuntimeException("(" + System.getProperty("pid") + ") Error, fail to rename " + file.getFile().getPath());
			try {
				file.close();
			} catch (Exception ignored) {
				log.info("Error closing file", ignored);
			}
			log.info("(" + System.getProperty("pid") + ") close file done");

		}
		processedFiles.clear();
		currentProcessedSize = 0;
	}

	private String getFilePath(String baseDir, String tag) {
		return FileUtil.ensurePathExists(baseDir + "/" + tag + "/");
	}

	private String getBackupFilePath(String backupDir, String tag) {
		return FileUtil.ensurePathExists(backupDir + "/" + tag + "/");
	}
}
