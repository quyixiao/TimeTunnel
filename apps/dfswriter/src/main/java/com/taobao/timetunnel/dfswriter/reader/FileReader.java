package com.taobao.timetunnel.dfswriter.reader;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.taobao.timetunnel.client.Message;
import com.taobao.timetunnel.client.message.MessageFactory;
import com.taobao.timetunnel.dfswriter.app.StoppableService;
import com.taobao.timetunnel.dfswriter.util.FileUtil;

public class FileReader extends StoppableService {
	public static void main(String args[]) throws Exception {
		String tag = "test";
		if (args.length >= 2)
			tag = args[1];
		String url = "hdfs://kgbtest4.corp.alimama.com:54310";
		if (args.length >= 3)
			url = args[2];
		String path = "d:/input";// "/user/tucuicui";
		if (args.length >= 4)
			path = args[3];
		String copies = "1";
		if (args.length >= 5)
			copies = args[4];
		RecordHandler handler = new HDFSHandler(tag, url, path, copies);
		String dir = "d:/test";
		if (args.length != 0)
			dir = args[0];
		FileReader reader = new FileReader(tag, 1024 * 1024 * 1000, 10 * 60 * 1000, new SimpleCommitPlanner(SimpleCommitPlanner.getCommitIntervalsPerHour(5), 1, 1, false), dir, dir + "/backup",
				handler, true, "*");
		// FileReader reader=new FileReader(tag,1024*1024*1000,10*60*1000,new
		// NoopCommitPlanner(),dir,dir+"/backup",handler);
		reader.start();
	}

	private static final Logger log = Logger.getLogger(FileReader.class);
	private String tag;
	private long maxCommitSize;
	private long autoCommitTimeout;
	private String baseDir;
	private String backupDir;
	private RecordHandler recordHandler;
	private CommitPlanner commitPlanner;
	private boolean isText;
	private String fileNameRegex;
	private List<LockableFile> processedFiles = new ArrayList<LockableFile>();
	private long currentProcessedSize = 0;
	private long lastFileProcessedTime = 0;

	public FileReader(String tag, long maxCommitSize, long autoCommitTimeout, CommitPlanner commitPlanner, String baseDir, String backupDir, RecordHandler handler, boolean isText, String fileNameRegex) {
		this.tag = tag;
		this.maxCommitSize = maxCommitSize;
		this.autoCommitTimeout = autoCommitTimeout;
		this.baseDir = baseDir;
		this.backupDir = backupDir;
		this.recordHandler = handler;
		this.commitPlanner = commitPlanner;
		this.isText = isText;
		this.fileNameRegex = fileNameRegex;
	}

	public void prepare() {
	}

	public void execute() {
		log.info("max commit size " + this.maxCommitSize);
		MultiPassMode multiPassMode = null;
		lastFileProcessedTime = System.currentTimeMillis();
		while (true) {
			for (File f : FileUtil.listDirectory(getFilePath(baseDir, tag), fileNameRegex)) {
				LockableFile file = null;
				try {
					if (isCommitRequired(f)) {
						recordHandler.commit(multiPassMode, 0);
						onCommit(multiPassMode);
						return;
					}
					if (isAlreadyOpened(f))
						continue;
					file = new LockableFile(f);
					file.lock();
					multiPassMode = commitPlanner.getMultiPassMode(f);
					recordHandler.setGroup(commitPlanner.getGroup(f, getFilePath(baseDir, tag)));
					doProcessFile(file, multiPassMode, 0, isText);
					processedFiles.add(file);
					lastFileProcessedTime = System.currentTimeMillis();
				} catch (OverlappingFileLockException e) {
					if (file != null) {
						if (file.getFile().length() == 0) {
							log.info("Delete garbage file " + file.getFile().getPath());
							file.getFile().delete();
						}
						file.close();
					}
				} catch (Exception e) {
					log.error("Exception: ", e);
					log.error("Exception happens while processing file " + f.getPath() + ", ignored", e);
					// ignore exceptions so that partial completed files can be
					// processed
					// throw new RuntimeException(e);
				}
			}
			try {
				Thread.sleep(5000);
			} catch (Exception ignored) {
			}
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

	private void doProcessFile(LockableFile file, MultiPassMode multiPassMode, int pass, boolean isText) {
		try {
			if (!isText)
				processFile(file, multiPassMode, pass);
			else
				processTextFile(file, multiPassMode, pass);
		} catch (Exception e) {
			log.error("Exception occured while processing " + file.getFile().getPath() + ", ignored", e);
		}
	}

	private static boolean readFully(FileChannel fc, ByteBuffer bb) throws IOException {
		while (bb.hasRemaining())
			if (fc.read(bb) == -1)
				if (bb.position() == 0) {
					return false;
				} else
					throw new EOFException();
		return true;
	}

	private void processFile(LockableFile file, MultiPassMode multiPassMode, int pass) throws IOException {
		log.info("(" + System.getProperty("pid") + ") process " + file.getFile().getPath());
		if (file.getFile().length() == 1) {
			recordHandler.handleRecord(null);
			return;
		}
		byte[] sizeArray = new byte[4];
		ByteBuffer sizeBb = ByteBuffer.wrap(sizeArray);
		while (readFully(file.getChannel(), sizeBb)) {
			sizeBb.flip();
			int size = sizeBb.getInt();
			byte[] data = new byte[size];
			ByteBuffer dataBb = ByteBuffer.wrap(data);
			readFully(file.getChannel(), dataBb);
			sizeBb.clear();
			Message message = MessageFactory.getInstance().createMessageFrom(data);
			Long actualBytesWritten = null;
			if (commitPlanner.checkMessage(message, multiPassMode, pass))
				actualBytesWritten = recordHandler.handleRecord(message);
			if ((!multiPassMode.isMultiPassEnabled() || pass == 0) && actualBytesWritten != null)
				currentProcessedSize = actualBytesWritten;
		}
	}

	@SuppressWarnings("unused")
	private static CharsetDecoder decoder = Charset.defaultCharset().newDecoder();

	private void processTextFile(LockableFile file, MultiPassMode multiPassMode, int pass) throws IOException {
		log.info("(" + System.getProperty("pid") + ") process textfile " + file.getFile().getPath());
		BufferedReader reader = new BufferedReader(Channels.newReader(file.getChannel(), "UTF-8"));
		String line = null;
		while ((line = reader.readLine()) != null) {

			Message message = MessageFactory.getInstance().createMessage(this.tag, line.getBytes("UTF-8"), "127.0.0.1", System.currentTimeMillis());
			Long actualBytesWritten = null;
			actualBytesWritten = recordHandler.handleRecord(message);
			if ((!multiPassMode.isMultiPassEnabled() || pass == 0) && actualBytesWritten != null)
				currentProcessedSize = actualBytesWritten;
		}
	}

	private void onCommit(MultiPassMode multiPassMode) throws IOException {
		if (processedFiles.size() == 0)
			return;
		if (multiPassMode.isMultiPassEnabled())
			performSecondPass(multiPassMode);
		recycleProcessedFiles();
	}

	private void performSecondPass(MultiPassMode multiPassMode) throws IOException {
		recordHandler.setGroup(commitPlanner.getSecondPassGroup(multiPassMode));
		for (LockableFile file : processedFiles)
			doProcessFile(file, multiPassMode, 1, isText);
		recordHandler.commit(multiPassMode, 1);
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

	public void shutdown() {
	}
}
