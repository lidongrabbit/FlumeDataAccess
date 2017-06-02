package com.asiainfo.ocdp.beijing.source;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.apache.log4j.Logger;

import com.asiainfo.ocdp.beijing.socket.BeijingSocketServer;
import com.asiainfo.ocdp.common.Constants;

/**
 * Created by yangjing5 on 2016/4/18.
 */
public class BeijingMcSource extends AbstractSource implements Configurable, PollableSource {
	private final static Logger logger = Logger.getLogger(BeijingMcSource.class);
	public static LinkedBlockingQueue<byte[]> msgQueue = new LinkedBlockingQueue<byte[]>(Integer.MAX_VALUE);
	private int analysisNum = 100;
	private String socketPort;
	private static int flag = 0;

	/**
	 * 初始化runtime.properties参数
	 */
	public void configure(Context context) {
		InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(Constants.PROPERTIE_FILENAME);
		Properties props = new Properties();
		try {
			props.load(inputStream);
			analysisNum = context.getInteger("maxworks",
					Integer.parseInt(props.getProperty(Constants.ANALYSIS_THREAD_NUM)));

			socketPort = context.getString("port", props.getProperty(Constants.SOCKETSERVER_PORT));
			logger.info("socketPort =" + socketPort);
			logger.info("load config [" + Constants.PROPERTIE_FILENAME + "]");

		} catch (IOException e) {
			logger.error("could not load config", e);
		}
	}

	@Override
	public void start() {
		String[] ports = socketPort.split(" ");

		for (String port : ports) {
			logger.info("port is " + port);
			new Thread(new BeijingSocketServer(port, "mc")).start();
		}
		logger.info("server socket,analysis task start...");
	}

	@Override
	public void stop() {
		// Disconnect from external client and do any additional cleanup
		// (e.g. releasing resources or nulling-out field values) ..
		super.stop();
	}

	// @Override
	public Status process() {
		try {
			flag++;
			byte[] msg = msgQueue.take();
			long startTime = System.currentTimeMillis();

			logger.info("msg:" + msg);
			String message = new String(msg);
			logger.info("message:" + message);
			logger.info("message index of \t:" + message.indexOf("\t"));
			logger.info("message index of \r:" + message.indexOf("\r"));
			logger.info("message index of \n:" + message.indexOf("\n"));
			logger.info("message index of \r\n:" + message.indexOf("\r\n"));
			String[] messages = message.split(Constants.MESSAGE_SEPARATOR);
			// String[] messages = message.split(Constants.MESSAGE_SEPARATOR);
			logger.info("message.size:" + messages.length);

			for (String signalling : messages) {
				Map<String, String> headers = new HashMap();
				// 第3字段为imsi,作为key
				String imsi = signalling.trim().split(Constants.HEADERS_KEY_SEPARATOR)[2];
				try {
					headers.put(Constants.HEADERS_KEY, imsi);
				} catch (StringIndexOutOfBoundsException e) {
					logger.error("The message format is invalid. <" + signalling + ">");
					continue;
				}
				// 去除imsi为0的无效信令
				if (!imsi.equals("000000000000000")) {
					Event e = EventBuilder.withBody(signalling.getBytes(), headers);
					getChannelProcessor().processEvent(e);
				}
			}

			long endTime = System.currentTimeMillis(); // 获取结束时间
			logger.info("End time : " + new Date());

			logger.info("Take time: " + (endTime - startTime) + "ms");
			return Status.READY;
		} catch (InterruptedException e) {
			e.printStackTrace();
			return Status.BACKOFF;
		}
	}
}