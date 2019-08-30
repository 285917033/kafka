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
	// ����һ����־����
		private final static Logger LOG = LoggerFactory.getLogger(JConsumerMutil.class);
		private final KafkaConsumer<String, String> consumer; // ����һ��������ʵ��
		private ExecutorService executorService; // ����һ���̳߳ؽӿ�

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

		/** ִ�ж��߳�������ʵ��. */
		public void execute() {
			// ��ʼ���̳߳�
			executorService = Executors.newFixedThreadPool(6);
			while (true) {
				// ��ȡKafka������Ϣ����
				ConsumerRecords<String, String> records = consumer.poll(100);
				if (null != records) {
					executorService.submit(new KafkaConsumerThread(records, consumer));
				}
			}
		}

		/** �ر�������ʵ��������̳߳� */
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

		/** �������߳�ʵ��. */
		class KafkaConsumerThread implements Runnable {

			private ConsumerRecords<String, String> records;

			public KafkaConsumerThread(ConsumerRecords<String, String> records, KafkaConsumer<String, String> consumer) {
				this.records = records;
			}

			public void run() {
				for (TopicPartition partition : records.partitions()) {
					// ��ȡ���Ѽ�¼���ݼ�
					List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
					LOG.info("Thread id : "+Thread.currentThread().getId());
					// ��ӡ���Ѽ�¼
					for (ConsumerRecord<String, String> record : partitionRecords) {
						System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
					}
				}
			}

		}

		/** ���߳�������ʵ�����. */
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
