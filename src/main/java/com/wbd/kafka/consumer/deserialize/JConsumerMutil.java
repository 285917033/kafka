package com.wbd.kafka.consumer.deserialize;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JConsumerMutil {
	// 创建一个日志对象
		private final static Logger LOG = LoggerFactory.getLogger(JConsumerMutil.class);
		private final KafkaConsumer<String, String> consumer; // 声明一个消费者实例
		private ExecutorService executorService; // 声明一个线程池接口

		public JConsumerMutil() {
			Properties prop = new Properties();
			prop.put("bootstrap.servers", "dn1:9092,dn2:9092,dn3:9092");
			prop.put("group.id", "group-4");
			prop.put("enable.auto.commit", "true");
			prop.put("auto.commit.interval.ms", "1000");
			prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			prop.put("value.deserializer", "com.wbd.kafka.consumer.deserialize.SalaryDeserializer");
			consumer = new KafkaConsumer<String, String>(prop);
			consumer.subscribe(Arrays.asList("exchanger"));
		}

		/** 执行多线程消费者实例. */
		public void execute() {
			// 初始化线程池
			executorService = Executors.newFixedThreadPool(6);
			while (true) {
				// 拉取Kafka主题消息数据
				ConsumerRecords<String, String> records = consumer.poll(100);
				if (null != records) {
					executorService.submit(new KafkaConsumerThread(records, consumer));
				}
			}
		}

		/** 关闭消费者实例对象和线程池 */
		public void shutdown() {
			try {
				if (consumer != null) {
					consumer.close();
				}
				if (executorService != null) {
					executorService.shutdown();
				}
				if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
					LOG.error("Shutdown kafka consumer thread timeout.");
				}
			} catch (InterruptedException ignored) {
				Thread.currentThread().interrupt();
			}
		}

		/** 消费者线程实例. */
		class KafkaConsumerThread implements Runnable {

			private ConsumerRecords<String, String> records;

			public KafkaConsumerThread(ConsumerRecords<String, String> records, KafkaConsumer<String, String> consumer) {
				this.records = records;
			}

			public void run() {
				for (TopicPartition partition : records.partitions()) {
					// 获取消费记录数据集
					List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
					LOG.info("Thread id : "+Thread.currentThread().getId());
					// 打印消费记录
					for (ConsumerRecord<String, String> record : partitionRecords) {
						System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
					}
				}
			}

		}

		/** 多线程消费者实例入口. */
		public static void main(String[] args) {
			JConsumerMutil consumer = new JConsumerMutil();
			try {
				consumer.execute();
			} catch (Exception e) {
				LOG.error("Mutil consumer from kafka has error,msg is " + e.getMessage());
				consumer.shutdown();
			}
		}
}
