package tk.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;


public class KafkaConsumertest4 {

	private final Consumer<String, String> consumer;
	private static Logger logger = Logger.getLogger(KafkaConsumertest4.class);
	
	public KafkaConsumertest4() {
		Properties kafkaProps = new Properties();
    	//set broker
    	kafkaProps.put("bootstrap.servers", "kafka1:9092,kafka2:9093,kafka3:9094");
    	kafkaProps.put("group.id","test1");
    	//set deserializer
    	kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    	kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    	
    	consumer = new KafkaConsumer<String,String>(kafkaProps);
	}
	
	public static void main(String[] args) {
		KafkaConsumertest4 consumer = new KafkaConsumertest4();
		consumer.receiveMsg1();
	}
	
	public void receiveMsg1() {
		List<PartitionInfo> partitionInfos = null;
		partitionInfos = consumer.partitionsFor("test1");
		List<TopicPartition> partitions = new ArrayList<>();
		
		if(partitionInfos != null) {
			for(PartitionInfo partition : partitionInfos) {
				partitions.add(new TopicPartition(partition.topic(),partition.partition()));
			}
			
			//set the partitions by itself
			consumer.assign(partitions);
			
			//set the offset if needed
			for(TopicPartition topicPartition : consumer.assignment()) {
				consumer.seek(topicPartition, 0);
			}
		}
		
		try {
			while(true) {
				//fetch messages
				ConsumerRecords<String,String> records = consumer.poll(1000);
				for(ConsumerRecord<String,String> record : records) {
					//process messages
					logger.debug(System.out.format("topic = %s, partition = %s, offset = %d, customer = %s, msg = %s\n",
								 record.topic(),record.partition(),record.offset(),record.key(),record.value()
							));
					
					
				}
				//commit
				consumer.commitSync();
			}
		}finally {
			consumer.close();
		}
	}
}
