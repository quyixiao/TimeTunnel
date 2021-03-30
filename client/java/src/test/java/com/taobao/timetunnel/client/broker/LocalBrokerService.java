package com.taobao.timetunnel.client.broker;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;

import com.taobao.timetunnel.client.util.SleepUtils;
import com.taobao.timetunnel.thrift.gen.ExternalService;
import com.taobao.timetunnel.thrift.gen.ExternalService.Processor;

/**
 * 
 * @author <jiugao@taobao.com>
 * @created 2010-10-8
 * 
 */
public class LocalBrokerService {
	private static Logger log = Logger.getLogger(LocalBrokerService.class);
	private static TServer server;
	private final static AtomicBoolean started = new AtomicBoolean(false);

	public synchronized static void bootstrap(int port, BrokerImpl bi) {
		if (started.get()) {
			stop();
		}
		try {
			Processor processor = new ExternalService.Processor(bi);
			TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(port);
			THsHaServer.Options options = new THsHaServer.Options();
			options.workerThreads = 1;
			server = new THsHaServer(processor, serverTransport, options);
			new Thread() {
				public void run() {
					server.serve();
				}
			}.start();
			log.debug("Local broker started at: " + port);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		started.set(true);
	}

	public static void openClose(int port, BrokerImpl bi) {
		if (started.get())
			stop();
		else
			bootstrap(port, bi);
	}

	public synchronized static void stop() {
		if (started.get()){
			server.stop();
			SleepUtils.sleep(1000);
			started.set(false);
			log.debug("Local broker has been stoped.");
		}
	}

	public static void main(String[] args) {
		bootstrap(Integer.parseInt(args[0]), new BrokerImpl());
	}

}
