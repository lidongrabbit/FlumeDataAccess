package com.asiainfo.ocdp.common;

import kafka.producer.Partitioner;

/**
 * kafka hash 分区
 */
public class HashPartitioner implements Partitioner {

	// @Override
	public int partition(Object key, int numPartitions) {
		int partition;
		String _key = (String) key;
		partition = Math.abs(_key.hashCode()) % numPartitions;
		return partition;
	}
}
