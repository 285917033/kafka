package com.wbd.kafka.consumer.deserialize;

import java.io.UnsupportedEncodingException;

import org.apache.kafka.common.serialization.Deserializer;

/**
 * 在消费者时，需要将二进制数据进行反序列化
 * 
 * @author jwh
 *
 */
public class SalaryDeserializer implements Deserializer<Object> {

	public Object deserialize(String topic, byte[] data) {
		try {
			return new String(data, "utf-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

}
