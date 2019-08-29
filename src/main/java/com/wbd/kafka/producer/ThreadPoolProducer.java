package com.wbd.kafka.producer;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.alibaba.fastjson.JSONObject;

/**
 * 多线程发送消息到kafka的broker中
 * 
 * @author jwh
 *
 */
public class ThreadPoolProducer extends Thread {

	/**
	 * 配置kafka连接信息
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
		prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return prop;

	}

	@Override
	public void run() {
		System.out.println("current==="+Thread.currentThread().getName());
		Producer<String, String> producer = new KafkaProducer<String, String>(configure());
		// 异步发送100条信息到指定的 主题中
		for (int i = 0; i < 10000; i++) {
			JSONObject json = new JSONObject();
			json.put("id", 1);
			json.put("ip", "192.168.1.1");
			json.put("date", new Date().toString());
			String key = "key" + i;
			producer.send(new ProducerRecord<String, String>("login", key, json.toString()), new Callback() {
				// 异步发送消息完成之后，调用的方法
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
			sleep(2000);
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
