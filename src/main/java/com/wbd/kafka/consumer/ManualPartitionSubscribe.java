package com.wbd.kafka.consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 * 消费者， 通过subscribe/assign来订阅主题
 * subscribe为自定义分区
 * assign为手动分区
 * @author jwh
 *
 */
public class ManualPartitionSubscribe extends Thread {

	public Properties configure() {

		Properties prop = new Properties();
		prop.put("bootstrap.servers", "dn1:9092,dn2:9092,dn3:9092");
		prop.put("group.id", "group-1");
		prop.put("enable.auto.commit", "true");
		prop.put("auto.commit.interval.ms", "1000");
		prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		return prop;

	}

	@Override
	public void run() {
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(configure());
		// 设置自定义分区
		TopicPartition tp = new TopicPartition("order",2);
		
		consumer.assign(Arrays.asList(tp));
		

		boolean flag = true;
		while (flag) {

			// 获取主题数据
			ConsumerRecords<String, String> records = consumer.poll(100);
			
			for (ConsumerRecord<String, String> record : records) {
				
				System.out.println("key="+record.key()+",value="+record.value());
			}
			
		}
		
		consumer.close();

	}

	public static void main(String[] args) {
		ManualPartitionSubscribe cs = new ManualPartitionSubscribe();
		cs.start();
	}
}
