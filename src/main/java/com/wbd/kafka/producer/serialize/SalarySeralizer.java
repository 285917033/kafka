package com.wbd.kafka.producer.serialize;

import java.io.UnsupportedEncodingException;

import org.apache.kafka.common.serialization.Serializer;
/**
 * ���л��࣬ �̳�kafka�� serializer�ӿ�
 * @author jwh
 *
 */
public class SalarySeralizer implements Serializer<Salary> {

	/**
	 * ���л����Ѷ����ɶ��������飬 �����л��� �Ѷ��������ݱ�ɶ���
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
