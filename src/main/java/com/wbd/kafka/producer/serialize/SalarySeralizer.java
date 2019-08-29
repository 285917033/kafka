package com.wbd.kafka.producer.serialize;

import java.io.UnsupportedEncodingException;

import org.apache.kafka.common.serialization.Serializer;
/**
 * 序列化类， 继承kafka的 serializer接口
 * @author jwh
 *
 */
public class SalarySeralizer implements Serializer<Salary> {

	/**
	 * 序列化，把对象变成二进制数组， 反序列化， 把二进制数据变成对象
	 */
	public byte[] serialize(String topic, Salary data) {
		
		try {
			return data.toString().getBytes("utf-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		
		return null;
	}

}
