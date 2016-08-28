package kafka.producers;

import kafka.producer.Partitioner;

public class SimplePartitioner implements Partitioner {

	public int partition(Object key, int a_numPartitions) {
		int partition = 0;
		String partitionKey = (String) key;
		int offset = partitionKey.lastIndexOf('.');
		if (offset > 0) {
			partition = Integer.parseInt(partitionKey.substring(offset + 1)) % a_numPartitions;
		}
		return partition;
	}

}
