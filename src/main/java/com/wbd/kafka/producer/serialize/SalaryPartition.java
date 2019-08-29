package com.wbd.kafka.producer.serialize;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

/**
 * 在分布式环境中。默认主题分区策略不能很好的满足业务需求，
 * 故此需要自定义
 * 
 * 自定义主题分区策略， key的hash 绝对值 %主题的分区数
 * 
 * 1.实现partitioner接口， 重新partition分区算法，
 * 2.在生产者程序中，设置分区类为自定义的分区类
 * @author jwh
 *
 */
public class SalaryPartition implements Partitioner {

	public void configure(Map<String, ?> configs) {
	
		
	}

	/**
	 * 实现kafka主题分区索引算法
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
