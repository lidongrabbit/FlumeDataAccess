package com.asiainfo.ocdp.sink;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 西安4G接入到kafka的MessagePreprocessor实现类.
 */
public class CustomMessagePreprocessor implements MessagePreprocessor {
	private static final Logger logger = LoggerFactory.getLogger(CustomMessagePreprocessor.class);

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
	// @Override
	public String transformMessage(String messageBody) {
		StringBuilder sb = new StringBuilder();
		if (StringUtils.isNotEmpty(messageBody)) {
			String[] msg = messageBody.split("\\|");
			sb.append(msg[1]).append("|");// City :912
			sb.append(TimeStamp2Date(msg[9])).append("|");// Procedure_Start_Time:1491011061129->yyyyMMddHHmmss
			sb.append(TimeStamp2Date(msg[10])).append("|");// Procedure_End_Time:1491011061129->yyyyMMddHHmmss
			sb.append(msg[7].replace("f", "")).append("|");// MSISDN:15162262783fffffffffffffffffffff->15162262783
			sb.append(msg[5]).append("|");// IMSI
			sb.append(msg[6]).append("|");// IMEI
			sb.append(msg[8]).append("|");// Procedure_Type
			sb.append(msg[1]).append("|");// City
			sb.append(msg[32]).append("|");// TAC
			sb.append(msg[33]).append("|");// Cell_ID
			sb.append(msg[34]).append("|");// Other_TAC
			sb.append(msg[35]).append("|");// Other_ECI
			sb.append("MME").append("|");// Interface
			sb.append("|");// toPhoneNum
			sb.append(msg[5]).append("|");// toimsi
			sb.append("|");// toimei
			sb.append("0").append("|");// calltime
			sb.append(msg[11]);// Procedure_Status
			return sb.toString();
		} else {
			return messageBody;
		}
	}

	// 将unix时间转成格式化时间
	public String TimeStamp2Date(String timestampString) {
		Long timestamp = Long.parseLong(timestampString);
		String date = new java.text.SimpleDateFormat("yyyyMMddHHmmss").format(new java.util.Date(timestamp));
		return date;
	}

	public String transformMessage(Event event, Context context) {
		// TODO Auto-generated method stub
		return null;
	}
}
