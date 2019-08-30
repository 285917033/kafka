package com.wbd.kafka.consumer.deserialize;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 * 一个消费者组中， 多个消费者应用程序消费一个topic的消息 可以提高并发吞吐量， 当一个消费者组中存在多个消费者程序来消费主题是，消费者组中的
 * 消费者程序会读取不同分区上的消息，
 * 
 * 如果一个消费者订阅了主题， 那么该主题中的每个分区只能分配给消费者组中的某一个消费者程序。
 * 
 * @author jwh
 *
 */
public class ConsumerMutil {
	
	public static void main(String[] args) {
		ConsumerMutil cm = new ConsumerMutil();
		
		try {
			cm.execute();
		} catch (Exception e) {
			// TODO: handle exception
			cm.shutdown();
		}
	}

	private final KafkaConsumer<String, String> consumer;

	private ExecutorService executorService;

	public ConsumerMutil() {
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

	public void execute() {

		executorService = Executors.newFixedThreadPool(6);
		while (true) {
			// 拉取kafka主题消息数据,有多条，然后启用多个线程去获取，也就是多个消费者去读取消息
			ConsumerRecords<String, String> records = consumer.poll(100);
           if(records!=null) {
        	   executorService.submit(new KafkaConsumerThread(records));
           }
		}
	}
	
	
	//关闭资源
	public void shutdown() {
		
		if(consumer!=null) {
			consumer.close();
		}
		
		if(executorService!=null) {
			executorService.shutdown();
		}
		
		
		
	}

	class KafkaConsumerThread implements Runnable {

		private ConsumerRecords<String, String> records;

		public KafkaConsumerThread(ConsumerRecords<String, String> records) {
		 
			this.records = records;
		}

		public void run() {

			
			for (TopicPartition partition : records.partitions()) {

				// 获取消费者记录集数据,通过分区获取对应的就记录集
				List<ConsumerRecord<String, String>> ps = records.records(partition);

				System.out.println(" 当前线程id " + Thread.currentThread().getId());

				// 打印消费记录，循环输出记录中的所有数据

				for (ConsumerRecord<String, String> record : ps) {

					System.out.printf("offset=%d,key=%s,value=%s%n", record.offset(), record.key(), record.value());
				}
			}

		}
	}

}
