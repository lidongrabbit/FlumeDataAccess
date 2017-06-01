package com.asiainfo.ocdp.beijing.sink;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asiainfo.ocdp.sink.MessagePreprocessor;

/**
 * 北京4GS1-MME信令接入到kafka的MessagePreprocessor实现类.
 */
public class BeijingMessagePreprocessor implements MessagePreprocessor {
	private static final Logger logger = LoggerFactory.getLogger(BeijingMessagePreprocessor.class);

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
	 * 发往kafka前修改body中的message，取前25个字段
	 */
	// @Override
	public String transformMessage(String messageBody) {
		StringBuilder sb = new StringBuilder();
		if (StringUtils.isNotEmpty(messageBody)) {
			String[] msg = messageBody.split("\\|");
			sb.append(msg[0]).append("|");// Length
			sb.append(msg[1]).append("|");// Interface
			sb.append(msg[2]).append("|");// Local Province
			sb.append(msg[3]).append("|");// Local City
			sb.append(msg[4]).append("|");// Owner Province
			sb.append(msg[5]).append("|");// Owner City
			sb.append(msg[6]).append("|");// Roaming Type
			sb.append(msg[7]).append("|");// Interface
			sb.append(msg[8]).append("|");// IMSI
			sb.append(msg[9]).append("|");// IMEI
			sb.append(msg[10]).append("|");// MSISDN
			sb.append(msg[11]).append("|");// Procedure Type
			sb.append(TimeStamp2Date(msg[12])).append("|");// Procedure Start
															// Time
			sb.append(TimeStamp2Date(msg[13])).append("|");// Procedure End Time
			sb.append(TimeStamp2Date(msg[14])).append("|");// StartLocation-longitude
			sb.append(TimeStamp2Date(msg[15])).append("|");// StartLocation-latitude
			// 可能手机号这种要特殊处理
			// sb.append(msg[7].replace("f","")).append("|");//MSISDN
			// :15162262783fffffffffffffffffffff->15162262783
			sb.append(msg[16]).append("|");// Location Source
			sb.append(msg[17]).append("|");// Procedure Status
			sb.append(msg[18]).append("|");// eNB IP Add
			sb.append(msg[19]).append("|");// TAC
			sb.append(msg[20]).append("|");// Cell ID
			sb.append(msg[21]).append("|");// Other TAC
			sb.append(msg[22]).append("|");// Other ECI
			sb.append(msg[23]).append("|");// Train Type
			sb.append(msg[24]).append("|");// Card Type
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
