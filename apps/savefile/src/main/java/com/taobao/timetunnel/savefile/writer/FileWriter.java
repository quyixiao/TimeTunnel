package com.taobao.timetunnel.savefile.writer;

import static com.taobao.timetunnel.client.TimeTunnel.subscribe;
import static com.taobao.timetunnel.client.TimeTunnel.tunnel;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.taobao.timetunnel.client.Message;
import com.taobao.timetunnel.client.SubscribeFuture;
import com.taobao.timetunnel.client.message.IOSerializable;
import com.taobao.timetunnel.client.util.ClosedException;
import com.taobao.timetunnel.savefile.app.Conf;
import com.taobao.timetunnel.savefile.app.SaveFileApp;
import com.taobao.timetunnel.savefile.app.StoppableService;
import com.taobao.timetunnel.util.filter.ContentFilter;

/**
 * 
 * @author <a href=mailto:jiugao@taobao.com>jiugao</a>
 * @created 2010-12-20
 * 
 */
public class FileWriter extends StoppableService {
	private static Logger log = Logger.getLogger(FileWriter.class);

	public static interface WriteCompletionHandler {
		public void onCompletion(List<Message> messages, boolean isSuccess);
	}

	public static class OutputStreamStruct {
		public String filePath;
		public OutputStream stream;
		public long bytesWritten;
		public long messagesWritten;
	}

	private WriteCompletionHandler callback;
	private static OutputStreamManager outputStreamManager;
	private boolean serializable;
	private int samplingRate;
	private Random random = new Random(System.currentTimeMillis());
	private SubscribeFuture rcvFuture;

	@SuppressWarnings("static-access")
	public FileWriter(String topic, int timeout, int rcvSize, WriteCompletionHandler callback, int samplingRate) {
		this.callback = callback;
		this.outputStreamManager = OutputStreamManager.getInstance();
		this.serializable = Conf.getInstance().getSerializable();
		this.samplingRate = samplingRate;
		try {
			rcvFuture = subscribe(tunnel(topic, false, true, timeout, rcvSize));
		} catch (ClosedException e) {
			log.error("close has been called", e);
			System.exit(-1);
		}
	}

	public void prepare() {
		Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {
			@Override
			public void uncaughtException(Thread t, Throwable e) {
				log.error("{}", e);
				System.exit(-1);
			}
		});
	}

	public void execute() {
		try {
			List<Message> ms = rcvFuture.get(5, TimeUnit.SECONDS);
			process(ms);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void process(List<Message> messages) throws IOException {
		for (Message message : messages) {
			List<ContentFilter> contentFilters = SaveFileApp.filters.get(message.getTopic());
			if (contentFilters == null) {
				if ((random.nextInt(100) + 1) > samplingRate)
					continue;
				writeRaw(message, message.getTopic());
			} else {
				Message m = (Message) message;
				if (m.isCompressed())
					m.decompress();
				String content = new String(m.getContent(), "UTF-8");
				String[] lines = content.split("\n");
				for (ContentFilter cf : contentFilters) {
					writeFilted(lines, message.getTopic(), cf);
				}
			}
		}
		if (callback != null)
			callback.onCompletion(messages, true);
	}

	public void writeFilted(String[] lines, String topic, ContentFilter contentFilter) throws IOException {
		String tag = contentFilter.getClass().getSimpleName();
		String baseFileName = topic + "-" + tag;
		for (String line : lines) {
			String processedLine = null;
			try {
				processedLine = contentFilter.filter(line + "\n");
			} catch (Throwable t) {
				log.error("filter process error and ignore it", t);
				continue;
			}
			if (processedLine == null) {
				log.debug("this content has been filtered: " + line);
				continue;
			}
			log.debug("this content has been hitted: " + processedLine);
			byte[] bytes = processedLine.getBytes(Charset.forName("UTF-8"));
			outputStreamManager.write(bytes, baseFileName);
		}
	}

	public void writeRaw(IOSerializable serializable, String tag) throws IOException {
		if (this.serializable) {
			byte[] data = serializable.serialize();
			outputStreamManager.writeWithLen(data, tag);
		} else {
			Message m = (Message) serializable;
			if (m.isCompressed())
				m.decompress();
			outputStreamManager.write(m.getContent(), tag);
		}
	}

	public void shutdown() {
		log.info("shutdown receive future");
		rcvFuture.cancel();
	}
}
