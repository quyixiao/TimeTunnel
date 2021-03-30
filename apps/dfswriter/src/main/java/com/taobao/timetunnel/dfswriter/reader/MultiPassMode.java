package com.taobao.timetunnel.dfswriter.reader;
import java.util.Date;

public class MultiPassMode {
	public static enum Mode{
		OFF,
		ON_LOWER_ZONE,
		ON_UPPER_ZONE
	};
	public MultiPassMode(MultiPassMode.Mode mode,Date date) {
		this.mode=mode;
		this.baselineDate=date;
	}
	public MultiPassMode.Mode getMode() {
		return mode;
	}
	public boolean isMultiPassEnabled() {
		return mode!=Mode.OFF;
	}
	public boolean isOnLowerZone() {
		return mode==Mode.ON_LOWER_ZONE;
	}
	public boolean isOnUpperZone() {
		return mode==Mode.ON_UPPER_ZONE;
	}
	public Date getBaselineDate() {
		return baselineDate;
	}
	public String toString() {
		StringBuilder sb=new StringBuilder();
		sb.append("Mode=");
		sb.append(mode);
		sb.append(", BaselineDate=");
		sb.append(baselineDate);
		return new String(sb);
	}
	private Mode mode;
	private Date baselineDate;
}