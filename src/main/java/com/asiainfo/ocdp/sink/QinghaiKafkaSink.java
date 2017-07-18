package com.asiainfo.ocdp.sink;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.asiainfo.ocdp.sink.MessagePreprocessor;
import com.asiainfo.ocdp.common.Constants;;

/**
 * 自定义KafkaSink,基于kafka-0.10.0.1版本的新API.
 */
public class QinghaiKafkaSink extends AbstractSink implements Configurable {

	private static final Logger logger = LoggerFactory.getLogger(QinghaiKafkaSink.class);
	private Properties producerProps;
	private KafkaProducer<String, String> producer;
	// private Producer<String,String> producer;
	private MessagePreprocessor messagePreProcessor;
	private String topic;
	private Context context;
	private int batchSize;
	private List<ProducerRecord<String, String>> messageList;
	// private List<KeyedMessage<String, String>> messageList;
	private SinkCounter sinkCounter;

	// @Override
	public Status process() throws EventDeliveryException {
		Status result = Status.READY;
		Channel channel = getChannel();
		Transaction transaction = null;
		Event event = null;
		String eventKey = null;
		// KeyedMessage<String, String> data;
		ProducerRecord<String, String> data;
		try {
			long processedEvent = 0;
			transaction = channel.getTransaction();
			transaction.begin();// 事务开始
			messageList.clear();

			for (; processedEvent < batchSize; processedEvent++) {
				event = channel.take();// 从channel取出一个事件
				if (event == null) {
					result = Status.BACKOFF;
					break;
				}
				sinkCounter.incrementEventDrainAttemptCount();
				String eventBody = new String(event.getBody());
				// if the metadata extractor is set, extract the topic and the
				// key.
				if (messagePreProcessor != null) {
					eventBody = messagePreProcessor.transformMessage(eventBody);
					eventKey = messagePreProcessor.extractKey(event, context);
					topic = messagePreProcessor.extractTopic(event, context);
				}
				// log the event for debugging
				if (logger.isDebugEnabled()) {
					logger.debug("{Event} " + eventBody);
				}
				if (StringUtils.isEmpty(eventKey)) {
					data = new ProducerRecord<String, String>(topic, eventBody);
				} else {
					data = new ProducerRecord<String, String>(topic, eventKey, eventBody);
				}

				// publish
				messageList.add(data);
				// logger.info("Add data [" + eventBody + "] into
				// messageList,position:" + processedEvent);
			}

			if (processedEvent == 0) {
				sinkCounter.incrementBatchEmptyCount();
				result = Status.BACKOFF;
			} else {
				if (processedEvent < batchSize) {
					sinkCounter.incrementBatchUnderflowCount();
				} else {
					sinkCounter.incrementBatchCompleteCount();
				}
				sinkCounter.addToEventDrainAttemptCount(processedEvent);
				for (ProducerRecord record : messageList) {
					producer.send(record);
				}

				logger.info("Send MessageList to Kafka: [ message List size = " + messageList.size()
						+ ",processedEvent number = " + processedEvent + "] ");
			}
			transaction.commit();// batchSize个事件处理完成，一次事务提交
			sinkCounter.addToEventDrainSuccessCount(processedEvent);
			result = Status.READY;
		} catch (Exception ex) {
			transaction.rollback();
			String errorMsg = "Failed to publish event: " + event;
			logger.error(errorMsg);
			throw new EventDeliveryException(errorMsg, ex);
		} finally {
			transaction.close();
		}
		return result;
	}

	@Override
	public synchronized void start() {
		// instantiate the producer
		// ProducerConfig config = new ProducerConfig(producerProps);
		sinkCounter.start();
		producer = new KafkaProducer<String, String>(producerProps);

		// 初始化topic,在configure
		topic = StringUtils.defaultIfEmpty(producerProps.getProperty(Constants.CUSTOME_TOPIC_KEY_NAME),
				Constants.DEFAULT_TOPIC_NAME);

		super.start();
		sinkCounter.incrementConnectionCreatedCount();
	}

	@Override
	public synchronized void stop() {
		producer.close();
		sinkCounter.stop();
		super.stop();
		sinkCounter.incrementConnectionClosedCount();
	}

	// @Override
	public void configure(Context context) {
		this.context = context;
		// logger.info("this.context:" + Arrays.asList(this.context));
		Map<String, String> params = context.getParameters();
		producerProps = new Properties();

		batchSize = context.getInteger("batchSize", Constants.DEFAULT_BATCH_SIZE);
		logger.info("batchSize : " + batchSize);
		messageList = new ArrayList<ProducerRecord<String, String>>(batchSize);

		for (String key : params.keySet()) {
			String value = params.get(key).trim();
			key = key.trim();
			producerProps.put(key, value);

			if (logger.isDebugEnabled()) {
				logger.debug("Reading a Kafka Producer Property: key: " + key + ", value: " + value);
			}
		}

		if (sinkCounter == null) {
			sinkCounter = new SinkCounter(getName());
		}
		// get the message Preprocessor if set
		// 通过反射将配置文件中配置的 ShanxiMessagePreprocessor 实例化
		String preprocessorClassName = context.getString(Constants.PREPROCESSOR);
		if (preprocessorClassName != null) {
			try {
				Class preprocessorClazz = Class.forName(preprocessorClassName.trim());
				Object preprocessorObj = preprocessorClazz.newInstance();
				if (preprocessorObj instanceof MessagePreprocessor) {
					messagePreProcessor = (MessagePreprocessor) preprocessorObj;
				} else {
					String errorMsg = "Provided class for MessagePreprocessor does not implement "
							+ "'com.thilinamb.flume.sink.MessagePreprocessor'";
					logger.error(errorMsg);
					throw new IllegalArgumentException(errorMsg);
				}
			} catch (ClassNotFoundException e) {
				String errorMsg = "Error instantiating the MessagePreprocessor implementation.";
				logger.error(errorMsg, e);
				throw new IllegalArgumentException(errorMsg, e);
			} catch (InstantiationException e) {
				String errorMsg = "Error instantiating the MessagePreprocessor implementation.";
				logger.error(errorMsg, e);
				throw new IllegalArgumentException(errorMsg, e);
			} catch (IllegalAccessException e) {
				String errorMsg = "Error instantiating the MessagePreprocessor implementation.";
				logger.error(errorMsg, e);
				throw new IllegalArgumentException(errorMsg, e);
			}
		}

		if (messagePreProcessor == null) {
			// MessagePreprocessor is not set. So read the topic from the
			// config.
			topic = context.getString(Constants.TOPIC, Constants.DEFAULT_TOPIC);
			if (topic.equals(Constants.DEFAULT_TOPIC)) {
				logger.warn("The Properties 'metadata.extractor' or 'topic' is not set. Using the default topic name"
						+ Constants.DEFAULT_TOPIC);
			} else {
				logger.info("Using the static topic: " + topic);
			}
		}
	}
}
