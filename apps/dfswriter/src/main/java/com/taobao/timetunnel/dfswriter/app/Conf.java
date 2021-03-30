package com.taobao.timetunnel.dfswriter.app;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * 
 * @author <a href=mailto:jiugao@taobao.com>jiugao</a>
 * @created 2010-12-20
 * 
 */
public class Conf {
	private static Conf instance = new Conf();
	private static final Logger log = Logger.getLogger(Conf.class);

	static class Constant {
		static final String SEP = ",";
		static final String TAGS = "tags";
		static final String FIXED_TIME = "fixed_time";
		static final String HDFS_URL = "dfs_url";
		static final String HDFS_PATH = "dfs_path";
		static final String BASE_DIR = "base_dir";
		static final String BACKUP_DIR = "backup_dir";
		static final String DFS_REPLICA_COUNT = "dfs_replica_count";
		static final String DFS_MAX_FILE_SIZE = "dfs_max_file_size";
		static final String DFS_COMMIT_TIMEOUT = "dfs_commit_timeout";
		static final String DFS_COMMIT_INTERVAL = "dfs_commit_interval";
		static final String Multi_PASS_LOWER_INDEX = "multi_pass_lower_index";
		static final String Multi_PASS_UPPER_INDEX = "multi_pass_upper_index";
		static final String IS_TIME_SENSITIVE = "is_time_sensitive";
		static final String IS_TEXT = "is_text";
		static final String FILENAME_REGX = "filename_regex";
	}

	private Properties p;

	private Conf() {
		load();
	}

	private void load() {
		InputStream rs = Conf.class.getClassLoader().getResourceAsStream("dfswriter.conf");
		p = new Properties();
		try {
			p.load(rs);
		} catch (IOException e) {
			log.error("load conf error and exit", e);
			System.exit(-1);
		}
	}

	public static Conf getInstance() {
		return instance;
	}

	public String getBaseDir() {
		String baseDir = (String) p.get(Constant.BASE_DIR);
		if (baseDir == null || "".equals(baseDir.trim())) {
			throw new RuntimeException("base dir is null");
		}
		log.info("base dir from conf: " + baseDir);
		return baseDir;
	}

	public String getFixMode() {
		String fixMode = "true";
		String fixTime = (String) p.get(Constant.FIXED_TIME);
		if (fixTime == null || "".equals(fixTime.trim())) {
			fixMode = "false";
		}
		log.info("fix mode from conf: " + fixMode);
		return fixMode;
	}

	public String[] getTags() {
		String tags = (String) p.get(Constant.TAGS);
		if (tags == null || "".equals(tags.trim())) {
			throw new RuntimeException("tags is null");
		}
		log.info("tags from conf: " + tags);
		return tags.split(Constant.SEP);
	}

	public String getFixedTime() {
		String fixTime = (String) p.get(Constant.FIXED_TIME);
		if (fixTime == null || "".equals(fixTime.trim())) {
			fixTime = "00:01";
		}
		log.info("fixTime from conf: " + fixTime);
		return fixTime;
	}

	public String getDfsUrl() {
		String dfsUrl = (String) p.get(Constant.HDFS_URL);
		if (dfsUrl == null || "".equals(dfsUrl.trim())) {
			throw new RuntimeException("dfsUrl is null");
		}
		log.info("dfs url from conf: " + dfsUrl);
		return dfsUrl;
	}

	public String getDfsPath() {
		String dfsPath = (String) p.get(Constant.HDFS_PATH);
		if (dfsPath == null || "".equals(dfsPath.trim())) {
			throw new RuntimeException("dfsPath is null");
		}
		log.info("dfs path from conf: " + dfsPath);
		return dfsPath;
	}

	public String getBackupDir() {
		String backupDir = (String) p.get(Constant.BACKUP_DIR);
		if (backupDir == null || "".equals(backupDir.trim())) {
			throw new RuntimeException("backupDir is null");
		}
		log.info("backupDir from conf: " + backupDir);
		return backupDir;
	}

	public String getDfsReplicaCount() {
		String dfsReplicaCount = (String) p.get(Constant.DFS_REPLICA_COUNT);
		if (dfsReplicaCount == null || "".equals(dfsReplicaCount.trim())) {
			dfsReplicaCount = "3";
		}
		log.info("dfsReplicaCount from conf: " + dfsReplicaCount);
		return dfsReplicaCount;
	}

	public String getDfsMaxFileSize() {
		String dfsMaxFileSize = (String) p.get(Constant.DFS_MAX_FILE_SIZE);
		if (dfsMaxFileSize == null || "".equals(dfsMaxFileSize.trim())) {
			dfsMaxFileSize = "2147483648";
		}
		log.info("dfsMaxFileSize from conf: " + dfsMaxFileSize);
		return dfsMaxFileSize;
	}

	public String getDfsCommitTimeout() {
		String dfsCommitTimeout = (String) p.get(Constant.DFS_COMMIT_TIMEOUT);
		if (dfsCommitTimeout == null || "".equals(dfsCommitTimeout.trim())) {
			dfsCommitTimeout = "60000";
		}
		log.info("dfsCommitTimeout from conf: " + dfsCommitTimeout);
		return dfsCommitTimeout;
	}

	public String getDfsCommitInterval() {
		String dfsCommitInterval = (String) p.get(Constant.DFS_COMMIT_INTERVAL);
		if (dfsCommitInterval == null || "".equals(dfsCommitInterval.trim())) {
			dfsCommitInterval = "5";
		}
		log.info("dfsCommitInterval from conf: " + dfsCommitInterval);
		return dfsCommitInterval;
	}

	public String getMultiPassLowerIndex() {
		String ml = (String) p.get(Constant.Multi_PASS_LOWER_INDEX);
		if (ml == null || "".equals(ml.trim())) {
			ml = "0";
		}
		log.info("Multi_PASS_LOWER_INDEX from conf: " + ml);
		return ml;
	}

	public String getMultiPassUpperIndex() {
		String mu = (String) p.get(Constant.Multi_PASS_UPPER_INDEX);
		if (mu == null || "".equals(mu.trim())) {
			mu = "0";
		}
		log.info("Multi_PASS_UPPER_INDEX from conf: " + mu);
		return mu;
	}

	public String getIsTimeSensitive() {
		String isS = (String) p.get(Constant.IS_TIME_SENSITIVE);
		if (isS == null || "".equals(isS.trim())) {
			isS = "false";
		}
		log.info("IS_TIME_SENSITIVE from conf: " + isS);
		return isS;
	}

	public String getIsText() {
		String isT = (String) p.get(Constant.IS_TEXT);
		if (isT == null || "".equals(isT.trim())) {
			isT = "false";
		}
		log.info("IS_TEXT from conf: " + isT);
		return isT;
	}

	public String getFilenameRegex() {
		String fg = (String) p.get(Constant.FILENAME_REGX);
		if (fg == null || "".equals(fg.trim())) {
			fg = ".*";
		}
		log.info("FILENAME_REGX from conf: " + fg);
		return fg;
	}

	public static void main(String[] args) {
		Conf conf = Conf.getInstance();
		conf.getBackupDir();
		conf.getBaseDir();
		conf.getDfsCommitInterval();
		conf.getDfsCommitTimeout();
		conf.getDfsMaxFileSize();
		conf.getDfsPath();
		conf.getDfsReplicaCount();
		conf.getDfsUrl();
		conf.getFilenameRegex();
		conf.getFixedTime();
		conf.getFixMode();
		conf.getIsText();
		conf.getIsTimeSensitive();
		conf.getMultiPassLowerIndex();
		conf.getMultiPassUpperIndex();
		conf.getTags();

	}

}
