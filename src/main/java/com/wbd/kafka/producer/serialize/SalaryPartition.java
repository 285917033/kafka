package com.wbd.kafka.producer.serialize;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

/**
 * �ڷֲ�ʽ�����С�Ĭ������������Բ��ܺܺõ�����ҵ������
 * �ʴ���Ҫ�Զ���
 * 
 * �Զ�������������ԣ� key��hash ����ֵ %����ķ�����
 * 
 * 1.ʵ��partitioner�ӿڣ� ����partition�����㷨��
 * 2.�������߳����У����÷�����Ϊ�Զ���ķ�����
 * @author jwh
 *
 */
public class SalaryPartition implements Partitioner {

	public void configure(Map<String, ?> configs) {
	
		
	}

	/**
	 * ʵ��kafka������������㷨
	 */
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		
		int partition=0;
		String k = (String) key;
		
		partition = Math.abs(k.hashCode()) % cluster.partitionCountForTopic(topic);
		
		return partition;
	}

	public void close() {
		
		
	}
	
	
	

}
