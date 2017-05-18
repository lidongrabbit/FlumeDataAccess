package com.asiainfo.ocdp.common;

/**
 * 常量类
 */
public class Constants {
	public static final String PROPERTIE_FILENAME = "runtime.properties";// 配置文件
	public final static String ANALYSIS_THREAD_NUM = "analysis.thread.num";// 线程数
	public final static String SOCKETSERVER_PORT = "socketserver.port";// socketserver端口号

	public static final String DEFAULT_TOPIC = "default-flume-topic";// 默认kafka-topic
	public static final String PREPROCESSOR = "preprocessor";// context读取flume配置文件默认preprocessor名称
	public static final String TOPIC = null;
	public static final int DEFAULT_BATCH_SIZE = 100000;// 默认批次数据
	public static final String CUSTOME_TOPIC_KEY_NAME = "cmcc.topic.name";
	public static final String DEFAULT_TOPIC_NAME = "CMCC";
	public static final String DEFAULT_KEY = "key";
	
	public static final String HEADERS_KEY = "key";
	public static final String MESSAGE_SEPARATOR = "\n";
	public static final String HEADERS_KEY_SEPARATOR = "\\|";
}
