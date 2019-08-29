package com.wbd.kafka.consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * 消费者， 通过subscribe/assign来订阅主题
 * 
 * @author jwh
 *
 */
public class ConsumerSubscribe extends Thread {

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
		// 消费者订阅主题
		consumer.subscribe(Arrays.asList("order"));

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
		ConsumerSubscribe cs = new ConsumerSubscribe();
		cs.start();
	}
}
