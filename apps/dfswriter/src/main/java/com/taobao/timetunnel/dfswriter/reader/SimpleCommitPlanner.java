package com.taobao.timetunnel.dfswriter.reader;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.taobao.timetunnel.client.Message;
import com.taobao.timetunnel.client.message.MessageImpl;
import com.taobao.timetunnel.dfswriter.util.DateUtil;

public class SimpleCommitPlanner implements CommitPlanner {
	private static final Logger log = Logger.getLogger(SimpleCommitPlanner.class);

	public static void main(String args[]) throws Exception {
		SimpleCommitPlanner cp = new SimpleCommitPlanner(Arrays.asList(0, 15, 35, 41), 1, 1, true);
		File f1 = new File("c:/2010_06_12_23_55#xyz");
		f1.createNewFile();
		File f2 = new File("c:/2010_06_13_00_00#def");
		f2.createNewFile();
		File f3 = new File("c:/2010_06_13_00_30#def");
		f2.createNewFile();
		MultiPassMode m1 = cp.getMultiPassMode(f1);
		MultiPassMode m2 = cp.getMultiPassMode(f2);
		MultiPassMode m3 = cp.getMultiPassMode(f3);
		System.out.println(f1.getName());
		System.out.println(m1);
		System.out.println(cp.getGroup(f1, ""));
		System.out.println(cp.getSecondPassGroup(m1));
		System.out.println(f2.getName());
		System.out.println(m2);
		System.out.println(cp.getGroup(f2, ""));
		System.out.println(cp.getSecondPassGroup(m2));
		System.out.println(f3.getName());
		System.out.println(m3);
		System.out.println(cp.getGroup(f3, ""));
		System.out.println(cp.getSecondPassGroup(m3));
	}

	public static List<Integer> getCommitIntervalsPerHour(int interval) {
		if (60 / interval * interval != 60)
			throw new IllegalArgumentException("Not a valid interval " + interval);
		List<Integer> commitIntervalsPerHour = new ArrayList<Integer>();
		int p = 0;
		while (p != 60) {
			commitIntervalsPerHour.add(p);
			p += interval;
		}
		return commitIntervalsPerHour;
	}

	private List<Integer> commitIntervalsPerHour;
	private int multiPassLowerZoneIndex;
	private int multiPassUpperZoneIndex;
	private boolean isTimeSensitive;

	public SimpleCommitPlanner(List<Integer> commitIntervalsPerHour, int multiPassLowerIndex, int multiPassUpperIndex, boolean isTimeSensitive) {
		this.commitIntervalsPerHour = commitIntervalsPerHour;
		this.multiPassLowerZoneIndex = multiPassLowerIndex;
		this.multiPassUpperZoneIndex = multiPassUpperIndex;
		this.isTimeSensitive = isTimeSensitive;
	}

	public int getCommitIntervalIndex(int min) {
		for (int i = 0; i < commitIntervalsPerHour.size(); i++) {
			int low = commitIntervalsPerHour.get(i);
			int high = (i != commitIntervalsPerHour.size() - 1) ? commitIntervalsPerHour.get(i + 1) : 60;
			if (low <= min && min < high)
				return i;
		}
		return -1;
	}

	public int getCommitInterval(int min) {
		return commitIntervalsPerHour.get(getCommitIntervalIndex(min));
	}

	@SuppressWarnings("deprecation")
	public boolean isInNewerCommitIntervalThan(File f1, File f2) {
		Date d1 = parseTimeStamp(f1);
		Date d2 = parseTimeStamp(f2);
		log.debug("isInNewerCommitIntervalThan d1 " + d1 + " d2" + d2 + " and f1 name: " + f1.getName() + " f2: " + f2.getName());
		if ((d1.getYear() == d2.getYear()) && (d1.getMonth() == d2.getMonth()) && (d1.getDay() == d2.getDay()) && (d1.getHours() == d2.getHours())) {
			return getCommitInterval(f1) > getCommitInterval(f2);
		} else {
			if (d1.after(d2))
				return true;
			else
				return false;
		}

	}

	@SuppressWarnings("deprecation")
	public int getCommitInterval(File f) {
		return getCommitInterval(parseTimeStamp(f).getMinutes());
	}

	public String getGroup(File f, String baseDir) {
		Date date = parseTimeStamp(f);
		String timeStr = DateUtil.getTimeStampInHour('/', date);
		return timeStr + "/" + DateUtil.padding(getCommitInterval(f));
	}

	public String getSecondPassGroup(MultiPassMode multiPassMode) {
		Date date = multiPassMode.getBaselineDate();
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		if (multiPassMode.isOnUpperZone()) {
			cal.set(Calendar.MINUTE, commitIntervalsPerHour.get(commitIntervalsPerHour.size() - 1));
			cal.add(Calendar.HOUR_OF_DAY, -1);
		}
		// return DateUtil.getTimeStampInMin('/', cal.getTime());
		return DateUtil.getTimeStampInMin(cal.getTime());
	}

	@SuppressWarnings("deprecation")
	public MultiPassMode getMultiPassMode(File f) {
		Date date = parseTimeStamp(f);
		Date beginOfDate = DateUtil.beginOfDate(date);
		log.debug("getMultiPassMode beginofDate: " + beginOfDate + " for file: " + f.getName());
		int commitIntervalIndex = getCommitIntervalIndex(date.getMinutes());
		if (date.getHours() == 0 && commitIntervalIndex < multiPassUpperZoneIndex) {
			log.info("(" + System.getProperty("pid") + ") enter multi pass upper zone, " + beginOfDate);
			return new MultiPassMode(MultiPassMode.Mode.ON_UPPER_ZONE, beginOfDate);
		}
		if (date.getHours() == 23 && commitIntervalIndex >= commitIntervalsPerHour.size() - multiPassLowerZoneIndex) {
			log.info("(" + System.getProperty("pid") + ") enter multi pass lower zone" + DateUtil.nextDate(beginOfDate));
			return new MultiPassMode(MultiPassMode.Mode.ON_LOWER_ZONE, DateUtil.nextDate(beginOfDate));
		}
		return new MultiPassMode(MultiPassMode.Mode.OFF, beginOfDate);
	}

	public boolean checkMessage(Message m, MultiPassMode multiPassMode, int pass) {
		MessageImpl message = (MessageImpl) m;
		if (multiPassMode.isOnLowerZone()) {
			if (pass == 0) {
				if (message.getCreatedTime() >= multiPassMode.getBaselineDate().getTime())
					return false;
			} else if (pass == 1) {
				if (message.getCreatedTime() < multiPassMode.getBaselineDate().getTime())
					return false;
			}
		} else if (multiPassMode.isOnUpperZone()) {
			if (pass == 0) {
				if (message.getCreatedTime() < multiPassMode.getBaselineDate().getTime()) {
					return false;
				}
			} else if (pass == 1) {
				if (message.getCreatedTime() >= multiPassMode.getBaselineDate().getTime())
					return false;
			}
		} else if (isTimeSensitive && message.getCreatedTime() < multiPassMode.getBaselineDate().getTime()) {
			log.error("(" + System.getProperty("pid") + ") ERROR: discard message with outdated timestamp, " + multiPassMode + ", pass " + pass);
			return false;
		}
		return true;
	}

	private Date parseTimeStamp(File f) {
		String timeStr = f.getName().split("#")[0];
		return DateUtil.parseDate("_", timeStr);
	}

	@Override
	public String getBackupFilePath(File f, String baseDir, String backupDir) {
		return backupDir + f.getName();
	}
}
