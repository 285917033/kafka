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
 * һ�����������У� ���������Ӧ�ó�������һ��topic����Ϣ ������߲����������� ��һ�����������д��ڶ�������߳��������������ǣ����������е�
 * �����߳�����ȡ��ͬ�����ϵ���Ϣ��
 * 
 * ���һ�������߶��������⣬ ��ô�������е�ÿ������ֻ�ܷ�������������е�ĳһ�������߳���
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
			// ��ȡkafka������Ϣ����,�ж�����Ȼ�����ö���߳�ȥ��ȡ��Ҳ���Ƕ��������ȥ��ȡ��Ϣ
			ConsumerRecords<String, String> records = consumer.poll(100);
           if(records!=null) {
        	   executorService.submit(new KafkaConsumerThread(records));
           }
		}
	}
	
	
	//�ر���Դ
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

				// ��ȡ�����߼�¼������,ͨ��������ȡ��Ӧ�ľͼ�¼��
				List<ConsumerRecord<String, String>> ps = records.records(partition);

				System.out.println(" ��ǰ�߳�id " + Thread.currentThread().getId());

				// ��ӡ���Ѽ�¼��ѭ�������¼�е���������

				for (ConsumerRecord<String, String> record : ps) {

					System.out.printf("offset=%d,key=%s,value=%s%n", record.offset(), record.key(), record.value());
				}
			}

		}
	}

}
