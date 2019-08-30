package com.wbd.kafka.consumer.deserialize;

import java.io.UnsupportedEncodingException;

import org.apache.kafka.common.serialization.Deserializer;

/**
 * ��������ʱ����Ҫ�����������ݽ��з����л�
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
