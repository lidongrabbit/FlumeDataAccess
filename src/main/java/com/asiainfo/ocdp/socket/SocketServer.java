package com.asiainfo.ocdp.socket;

import org.apache.log4j.Logger;

import com.asiainfo.ocdp.source.FlumeSdtpSource;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by yangjing5 on 2016/4/18.
 */
public class SocketServer implements Runnable {
	public static LinkedBlockingQueue<Socket> socketQueue = new LinkedBlockingQueue<Socket>(Integer.MAX_VALUE);
	private final static Logger logger = Logger.getLogger(SocketServer.class);
	private String socketPort;
	private String decodeType;

	public SocketServer(String socketPort, String type) {
		this.socketPort = socketPort;
		this.decodeType = type;
	}

	// @Override
	public void run() {
		ServerSocket server = null;
		try {
			server = new ServerSocket(Integer.parseInt(socketPort));
		} catch (IOException e) {
			logger.error("could not listen ", e);
		}
		Socket socket = null;
		while (true) {
			try {

				socket = server.accept();
				socket.setReceiveBufferSize(10 * 10240 * 1024);
				if (decodeType.equals("binary")) {
					new Thread(new BinarySocketAnalysisTask(socket)).start();
				} else if (decodeType.equals("sdtp")) {
					new Thread(new SdtpSocketAnalysisTask(socket)).start();
				}
			} catch (IOException e) {
				logger.error("could not accept data from client", e);
			}
		}
	}
}
