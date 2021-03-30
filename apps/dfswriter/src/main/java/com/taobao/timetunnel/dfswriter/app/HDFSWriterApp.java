package com.taobao.timetunnel.dfswriter.app;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.taobao.timetunnel.dfswriter.reader.CommitPlanner;
import com.taobao.timetunnel.dfswriter.reader.FileReader;
import com.taobao.timetunnel.dfswriter.reader.FixModeFileReader;
import com.taobao.timetunnel.dfswriter.reader.HDFSHandler;
import com.taobao.timetunnel.dfswriter.reader.NoopCommitPlanner;
import com.taobao.timetunnel.dfswriter.reader.RecordHandler;
import com.taobao.timetunnel.dfswriter.reader.SimpleCommitPlanner;
import com.taobao.timetunnel.dfswriter.util.DateUtil;
import com.taobao.timetunnel.dfswriter.util.TimerUtil;

public class HDFSWriterApp {

	private static final Logger log = Logger.getLogger(HDFSWriterApp.class);
	private static FileReader reader;
	private FixModeTask fixModeTask = null;

	public void destroy() {
		log.info("shutdown.....");
		if (fixModeTask == null)
			reader.stop();
		else
			fixModeTask.stop();
	}

	public void start() {
		boolean fixMode = "true".compareToIgnoreCase(Conf.getInstance().getFixMode()) == 0;
		String[] tags = Conf.getInstance().getTags();
		String fixedTime = checkMode(fixMode, tags);
		String hdfsUrl = Conf.getInstance().getDfsUrl();
		String hdfsPath = Conf.getInstance().getDfsPath();
		String baseDir = Conf.getInstance().getBaseDir();
		String backupDir = Conf.getInstance().getBackupDir();
		String numOfReplicas = Conf.getInstance().getDfsReplicaCount();
		long maxFileSize = Long.valueOf(Conf.getInstance().getDfsMaxFileSize());
		long commitTimeout = Long.valueOf(Conf.getInstance().getDfsCommitTimeout());
		int commitInterval = Integer.valueOf(Conf.getInstance().getDfsCommitInterval());
		int multiPassLowerIndex = Integer.valueOf(Conf.getInstance().getMultiPassLowerIndex());
		int multiPassUpperIndex = Integer.valueOf(Conf.getInstance().getMultiPassUpperIndex());
		boolean isTimeSensitive = "true".compareToIgnoreCase(Conf.getInstance().getIsTimeSensitive()) == 0;
		boolean isText = ("true".compareToIgnoreCase(Conf.getInstance().getIsText())) == 0;
		String fileNameRegex = Conf.getInstance().getFilenameRegex();

		if (fixMode == false) {
			RecordHandler handler = new HDFSHandler(tags[0], hdfsUrl, hdfsPath, numOfReplicas);
			CommitPlanner commitPlanner = null;
			if (commitInterval == 0)
				commitPlanner = new NoopCommitPlanner();
			else
				commitPlanner = new SimpleCommitPlanner(SimpleCommitPlanner.getCommitIntervalsPerHour(commitInterval), multiPassLowerIndex, multiPassUpperIndex, isTimeSensitive);
			reader = new FileReader(tags[0], maxFileSize, commitTimeout, commitPlanner, baseDir, backupDir, handler, isText, fileNameRegex);
			reader.start();
		} else {
			fixModeTask = new FixModeTask(fixedTime, tags, hdfsUrl, hdfsPath, numOfReplicas, commitInterval, maxFileSize, commitTimeout, baseDir, backupDir, isTimeSensitive, fileNameRegex);
			new Thread(fixModeTask, "fixModeThread").start();
		}
	}

	private String checkMode(boolean fixMode, String[] tags) {
		String fixedTime;
		if (fixMode == false) {
			log.debug("UNFIXED MODE");
			fixedTime = null;
			if (tags.length > 1) {
				throw new RuntimeException("multi topics not supported at unfix mode");
			}
		} else {
			log.info("FIXED MODE");
			fixedTime = Conf.getInstance().getFixedTime();
			log.debug("fix mode and fixedTime is " + fixedTime);
		}
		return fixedTime;
	}

	public Runnable createShutdownHook() {
		return new ShutdownJob();
	}

	class ShutdownJob implements Runnable {
		@Override
		public void run() {
			destroy();
		}
	}

	public static void main(String[] args) {
		HDFSWriterApp app = new HDFSWriterApp();
		Runtime.getRuntime().addShutdownHook(new Thread(app.createShutdownHook(), "shutdown hook"));
		app.start();
	}

	private static class FixModeTask implements Runnable {
		private final String fixedTime;
		private final String[] tags;
		private final String hdfsUrl;
		private final String hdfsPath;
		private final String numOfReplicas;
		private final int commitInterval;
		private final long maxFileSize;
		private final long commitTimeout;
		private final String baseDir;
		private final String backupDir;
		private final boolean isTimeSensitive;
		private final AtomicBoolean stop = new AtomicBoolean(false);
		private Thread sleepThread;
		private String fileNameRegx;
		private FixModeFileReader fixModeReader;

		public FixModeTask(final String fixedTime, String[] tags, String hdfsUrl, String hdfsPath, String numOfReplicas, int commitInterval, long maxFileSize, long commitTimeout, String baseDir,
				String backupDir, boolean isTimeSensitive, String fileNameRegx) {
			super();
			this.fixedTime = fixedTime;
			this.tags = tags;
			this.hdfsUrl = hdfsUrl;
			this.hdfsPath = hdfsPath;
			this.numOfReplicas = numOfReplicas;
			this.commitInterval = commitInterval;
			this.maxFileSize = maxFileSize;
			this.commitTimeout = commitTimeout;
			this.baseDir = baseDir;
			this.backupDir = backupDir;
			this.isTimeSensitive = isTimeSensitive;
			this.fileNameRegx = fileNameRegx;
		}

		public void stop() {
			if (this.sleepThread != null)
				this.sleepThread.interrupt();
			this.stop.set(true);
		}

		private void timer(final String fixedTime) {
			this.sleepThread = new Thread(new Runnable() {
				public void run() {
					TimerUtil.sleep2(fixedTime);
				}
			}, "sleep_thread");
			this.sleepThread.start();
			while (this.sleepThread.isAlive()) {
				try {
					this.sleepThread.join();
				} catch (InterruptedException ignore) {
				}
			}
		}

		@Override
		public void run() {
			while (!stop.get()) {
				log.debug("sleep to fixedTime and start to work");
				for (String tag : tags) {
					log.error("begin process tag: " + tag);
					RecordHandler handler = new HDFSHandler(tag, hdfsUrl, hdfsPath, numOfReplicas);
					CommitPlanner commitPlanner = null;
					if (commitInterval == 0)
						commitPlanner = new NoopCommitPlanner();
					else
						commitPlanner = new SimpleCommitPlanner(SimpleCommitPlanner.getCommitIntervalsPerHour(commitInterval), 0, 0, isTimeSensitive);
					fixModeReader = new FixModeFileReader(tag, maxFileSize, commitTimeout, commitPlanner, baseDir, backupDir, handler, DateUtil.preDate('_'), fileNameRegx);
					fixModeReader.execute();
					log.error("prcess tag: " + tag + " success");
				}
				timer(fixedTime);
			}
		}
	}
}
