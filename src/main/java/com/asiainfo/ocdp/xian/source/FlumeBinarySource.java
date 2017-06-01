package com.asiainfo.ocdp.xian.source;

import java.io.IOException;
import java.io.InputStream;
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

import com.asiainfo.ocdp.common.Constants;
import com.asiainfo.ocdp.common.ConvToByte;
import com.asiainfo.ocdp.socket.SocketServer;

/**
 * 数据源为二进制格式
 */
public class FlumeBinarySource extends AbstractSource implements Configurable, PollableSource {
	private final static Logger logger = Logger.getLogger(FlumeBinarySource.class);

	public static LinkedBlockingQueue<byte[]> msgQueue = new LinkedBlockingQueue<byte[]>(Integer.MAX_VALUE);
	private int analysisNum;
	private String socketPort;

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
			;
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
			new Thread(new SocketServer(port, msgQueue)).start();
		}
		logger.info("server socket,analysis task start...");
	}

	@Override
	public void stop() {
		super.stop();
	}

	public Status process() {
		int off;
		int flag = 0;
		int flage = 0;
		long startTime = System.currentTimeMillis();
		try {
			byte[] msg = msgQueue.take();
			// off =1 ,跳过一位buffer[0]，这一位字节表示，XDR数据类型：1：合成XDR数据 2：单接口XDR数据
			off = 1;
			// 因为每次传输的数据并不是一条记录，所以需要对数据进行拆分解析
			String[] messages = getMessages(msg, off).split(Constants.MESSAGE_SEPARATOR);
			for (String signalling : messages) {
				flag++;
				Map<String, String> headers = new HashMap();
				// 第6字段为imsi,作为key
				String imsi = signalling.trim().split(Constants.HEADERS_KEY_SEPARATOR)[5];
				try {
					headers.put(Constants.HEADERS_KEY, imsi);
				} catch (StringIndexOutOfBoundsException e) {
					logger.error("The message format is invalid. <" + signalling + ">");
					continue;
				}
				// 去除imsi为0的无效信令
				if (!(imsi.equals("000000000000000") || imsi.equals("ffffffffffffffff") || imsi == null
						|| imsi.equals(""))) {
					flage++;
					Event e = EventBuilder.withBody(signalling.getBytes(), headers);
					getChannelProcessor().processEvent(e);
				}
				// logger.info(" 第"+flag+"条记录是： "+signalling);
			}
			long endTime = System.currentTimeMillis(); // 获取结束时间
			logger.info("接收记录数：总共有" + flag + "条记录，其中有效的有" + flage + "条，所耗时间为" + (endTime - startTime) + "ms");
			return Status.READY;
		} catch (InterruptedException e) {
			e.printStackTrace();
			return Status.BACKOFF;
		}
	}

	// by jesusrui 转化2进制
	private String getMessages(byte[] buffer, int off) {
		StringBuilder sb = new StringBuilder();
		// 循环遍历，取出多条记录数据
		for (; off < buffer.length;) {
			// for(int i=0;i<buffer.length;i++){
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// Length
			off += 2;
			sb.append(ConvToByte.decodeTBCD(buffer, off, off + 2, true)).append("|");// City
			off += 2;
			sb.append(buffer[off] + "|");// Interface
			off += 1;
			sb.append(ConvToByte.getHexString(buffer, off, off + 16)).append("|");// XDR-ID
			off += 16;
			sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");// RAT
			off += 1;
			sb.append(ConvToByte.decodeTBCD(buffer, off, off + 8, true)).append("|");// IMSI
			off += 8;
			sb.append(ConvToByte.decodeTBCD(buffer, off, off + 8, true)).append("|");// IMEI
			off += 8;
			sb.append(ConvToByte.decodeTBCD(buffer, off, off + 16, false)).append("|");// MSISDN
			off += 16;
			sb.append((buffer[off] & 0xff)).append("|");// Procedure Type
			off += 1;
			sb.append(ConvToByte.byteToLong(buffer, off)).append("|");// ProcedureStartTime
			off += 8;
			sb.append(ConvToByte.byteToLong(buffer, off)).append("|");// ProcedureEndTime
			off += 8;
			sb.append((buffer[off] & 0xff)).append("|");// Procedure Status
			off += 1;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// RequestCause
			off += 2;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// FailureCause
			off += 2;
			sb.append((buffer[off] & 0xff)).append("|");// Keyword 1
			off += 1;
			sb.append((buffer[off] & 0xff)).append("|");// Keyword 2
			off += 1;
			sb.append((buffer[off] & 0xff)).append("|");// Keyword 3
			off += 1;
			sb.append((buffer[off] & 0xff)).append("|");// Keyword 4
			off += 1;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");// MME-UE-S1AP-ID
			off += 4;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// Old-MME-Group-ID
			off += 2;
			sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");// Old-MME-Code
			off += 1;
			sb.append(ConvToByte.getHexString(buffer, off, off + 4)).append("|");// Old-M-TMSI
			off += 4;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// MME-Group-ID
			off += 2;
			sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");// MMECode
			off += 1;
			sb.append(ConvToByte.getHexString(buffer, off, off + 4)).append("|");// M-TMSI
			off += 4;
			sb.append(ConvToByte.getHexString(buffer, off, off + 4)).append("|");// TMSI
			off += 4;
			sb.append(ConvToByte.getIpv4(buffer, off)).append("|");// USER_IPv4
			off += 4;
			sb.append(ConvToByte.getIpv6(buffer, off)).append("|");// USER_IPv6
			off += 16;
			sb.append(ConvToByte.getIp(buffer, off)).append("|");// MME IP Add
			off += 16;
			sb.append(ConvToByte.getIp(buffer, off)).append("|");// eNB IP Add
			off += 16;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// MMEPort
			off += 2;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// eNBPort
			off += 2;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// TAC
			off += 2;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");// CellID
			off += 4;
			sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// OtherTAC
			off += 2;
			sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");// OtherECI
			off += 4;
			sb.append(ConvToByte.getHexString(buffer, off, 32)).append("|");// APN
			off += 32;

			sb.append((buffer[off] & 0xff)).append("|");// VoiceDomain 新增字段
			off += 1;
			sb.append((buffer[off] & 0xff)).append("|");// Vopsopt 新增字段
			off += 1;

			int epsBearerNum = ConvToByte.byteToUnsignedByte(buffer, off);
			sb.append(epsBearerNum + "|");// EPS Bearer Number
			off += 1;
			for (int n = 0; n < epsBearerNum; n++) {
				sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");// Bearer-1-ID
				off += 1;
				sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");// Bearer-1-Type
				off += 1;
				sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");// Bearer-1-QCI
				off += 1;
				sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append("|");// Bearer-1-Status
				off += 1;
				sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// Bearer-1-RequestCause
				off += 2;
				sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append("|");// Bearer-1-Failure-Cause
				off += 2;
				sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");// Bearer-1-eNB-GTP-TEID
				off += 4;
				sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append("|");// Bearer-1-SGW-GTP-TEID
				off += 4;

			}
			sb.deleteCharAt(sb.lastIndexOf("|"));
			// 跳过换行符,并添加换行符，中兴的是 /n/r
			off += 1;
			sb.append("\n");
		}
		sb.deleteCharAt(sb.lastIndexOf("\n"));
		return sb.toString();
	}
}