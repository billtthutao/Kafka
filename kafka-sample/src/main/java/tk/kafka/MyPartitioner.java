package tk.kafka;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

public class MyPartitioner implements Partitioner{
	
	@Override
	public void configure(Map<String, ?> configs) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		// TODO Auto-generated method stub
		// fetch topic partitions information
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        // set partitions
        if(key.toString().equals("ProductStart")) {
            // put the message to the last partitions
            return numPartitions - 1;
        }
        String phoneNum = key.toString();
        if (numPartitions == 1)
        	return numPartitions;
        else
        	return phoneNum.substring(0, 3).hashCode() % (numPartitions - 1);
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
	
}
