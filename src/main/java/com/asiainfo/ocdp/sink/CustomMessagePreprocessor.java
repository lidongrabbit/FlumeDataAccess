package com.asiainfo.ocdp.sink;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 自定义MessagePreprocessor实现类.
 */
public class CustomMessagePreprocessor implements MessagePreprocessor {

	private static final Logger logger = LoggerFactory.getLogger(CustomMessagePreprocessor.class);
	private static final String DATETIME_FORMAT = "yyyyMMdd HH:mm:ss:SSS";

	// @Override
	public String extractKey(Event event, Context context) {
		String key = event.getHeaders().get("key");
		return key;
	}

	// @Override
	public String extractTopic(Event event, Context context) {
		// 优先从flume配置文件获取topic
		return context.getString("topic", "default-topic");
	}

	/**
	 * 发往kafka前修改body中的message
	 */
	public String transformMessage(String messageBody) {
		StringBuilder sb = new StringBuilder();
		if (StringUtils.isNotEmpty(messageBody)) {

			String[] msg = messageBody.split("\\|");

			sb.append(msg[1]).append("|"); // Local Province
			sb.append(msg[2]).append("|"); // Local City
			sb.append(msg[3]).append("|"); // Owner Province
			sb.append(msg[4]).append("|"); // Owner City
			sb.append(msg[5]).append("|"); // Roaming Type
			sb.append(msg[6]).append("|"); // Interface
			sb.append(msg[8]).append("|"); // RAT
			sb.append(msg[9]).append("|"); // IMSI
			sb.append(msg[10]).append("|"); // IMEI
			sb.append(msg[11]).append("|"); // MSISDN
			sb.append(msg[12]).append("|"); // Procedure Type
			sb.append(TimeStamp2Date(msg[13])).append("|"); // Procedure Start
															// Time
			sb.append(TimeStamp2Date(msg[14])).append("|"); // Procedure End
															// Time
			sb.append(msg[15]).append("|"); // StartLocation-longitude
			sb.append(msg[16]).append("|"); // StartLocation-latitude
			sb.append(msg[43]).append("|"); // TAC
			sb.append(msg[44]); // Cell ID

			return sb.toString();

		} else {
			return messageBody;
		}
	}

	// 将unix时间转成格式化时间
	public String TimeStamp2Date(String timestampString) {
		Long timestamp = Long.parseLong(timestampString);
		String date = new java.text.SimpleDateFormat(DATETIME_FORMAT).format(new java.util.Date(timestamp));
		return date;
	}

	public String transformMessage(Event event, Context context) {
		// TODO Auto-generated method stub
		return null;
	}
}
