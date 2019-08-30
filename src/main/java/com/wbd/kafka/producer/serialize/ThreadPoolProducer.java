package com.wbd.kafka.producer.serialize;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * ���̷߳�����Ϣ��kafka��broker��
 * 
 * @author jwh
 *
 */
public class ThreadPoolProducer extends Thread {

	/**
	 * ����kafka������Ϣ
	 * 
	 */

	public Properties configure() {

		Properties prop = new Properties();
		prop.put("bootstrap.servers", "dn1:9092,dn2:9092,dn3:9092");
		prop.put("acks", "0");
		prop.put("batch.size", 16384);
		prop.put("linger.ms", 1);
		prop.put("buffer.memory", 33554432);
		prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		// �����Զ������л���
		prop.put("value.serializer", "com.wbd.kafka.producer.serialize.SalarySeralizer");
		//�Զ��������
		prop.put("partitioner.class", "com.wbd.kafka.producer.serialize.SalaryPartition");
		return prop;

	}

	@Override
	public void run() {
		Producer<String, Salary> producer = new KafkaProducer<String, Salary>(configure());
		// �첽����100����Ϣ��ָ���� ������
      for(int i=600;i<10000;i++) {
		Salary s = new Salary();
		s.setId("id"+i);
		s.setSalary("нˮ"+i);

		producer.send(new ProducerRecord<String, Salary>("exchanger", "abc", s), new Callback() {
			// �첽������Ϣ���֮�󣬵��õķ�ConsumerSubscribe.java��
			public void onCompletion(RecordMetadata metadata, Exception exception) {

				if (exception != null) {
					System.out.println(("send error" + exception.getMessage()));

				} else {
					System.out.println(("send success" + metadata.offset()));

				}
			}

		});
      }

		try {
			sleep(4000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		producer.close();

	}

	public static void main(String[] args) {

		ExecutorService executorService = Executors.newFixedThreadPool(6);
		ThreadPoolProducer otp = new ThreadPoolProducer();
		executorService.submit(otp);
		executorService.shutdown();
	}
}
