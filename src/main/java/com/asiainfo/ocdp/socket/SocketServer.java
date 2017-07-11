package com.asiainfo.ocdp.socket;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

/**
 * Created by yangjing5 on 2016/4/18.
 */
public class SocketServer implements Runnable {
	public static LinkedBlockingQueue<Socket> socketQueue = new LinkedBlockingQueue<Socket>(Integer.MAX_VALUE);
	private final static Logger logger = Logger.getLogger(SocketServer.class);
	private String socketPort;

	public SocketServer(String port) {
		this.socketPort = port;
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
				new Thread(new SdtpSocketAnalysisTask(socket)).start();

			} catch (IOException e) {
				logger.error("could not accept data from client", e);
			}
		}
	}
}
