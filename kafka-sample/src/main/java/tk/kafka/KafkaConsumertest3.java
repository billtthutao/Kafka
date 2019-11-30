package tk.kafka;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.json.JSONObject;

public class KafkaConsumertest3 {
	
	private final Consumer<String, String> consumer;
	private static Logger logger = Logger.getLogger(KafkaConsumertest3.class);
	private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
	
	public KafkaConsumertest3() {
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
		// TODO Auto-generated method stub
		KafkaConsumertest3 consumer = new KafkaConsumertest3();
		consumer.receiveMsg1();
	}

	public void receiveMsg1() {
		//subscribe topic test1
		consumer.subscribe(Collections.singletonList("test1"),new SaveOffsetRebalance());
		Map<String, String> recordMap = new HashMap<>();
		
		//pull(0) does not fetch any message and just get the assignment of the partitions
		consumer.poll(0);
		
		//get the offset from DB for each partition
		for(TopicPartition topicPartition : consumer.assignment()) {
			consumer.seek(topicPartition, getOffsetFromDB(topicPartition));
		}
		
		try {
			while(true) {
				ConsumerRecords<String,String> records = consumer.poll(100);
				for(ConsumerRecord<String,String> record : records) {
					//process messages
					logger.debug(System.out.format("topic = %s, partition = %s, offset = %d, customer = %s, msg = %s\n",
								 record.topic(),record.partition(),record.offset(),record.key(),record.value()
							));
					
					if(record.key() != null) {
						recordMap.put(record.key(), record.value());
						currentOffsets.put(new TopicPartition(record.topic(),record.partition()), new OffsetAndMetadata(record.offset()+1,"no metadata"));
					}
					
					JSONObject json = new JSONObject(recordMap);
					
					logger.debug(json.toString(4));
					
					//store offset in DB
					storeOffsetInDB(record.topic(),record.partition(),record.offset());
				}
				
				commitDBTransaction();
			}
		}finally {
			//method 2 同步提交，不断重试，直到提交成功
			try {
				consumer.commitSync(currentOffsets);
			}catch(CommitFailedException e) {
				logger.error("commit failed", e);
			}finally {
				consumer.close();
			}
		}
	}
	
	private void storeOffsetInDB(String topic, int partition, long offset) {
		logger.debug(System.out.format("Offset %d in Partition %d of Topic: %s is saved in DB\n", offset,partition,topic));
	}
	
	private void commitDBTransaction() {
		logger.debug("commit Database Transaction\n");
	}
	
	private OffsetAndMetadata getOffsetFromDB(TopicPartition topicPartition) {
		return new OffsetAndMetadata(0,"no metadata");
	}
	
	private class SaveOffsetRebalance implements ConsumerRebalanceListener{

		//会在再均衡开始之前和消费者停止读取消息之后被调用
		@Override
		public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			// TODO Auto-generated method stub
			//commit DB offset changes
			logger.debug("onPartitionsRevoked\n");
			commitDBTransaction();
		}

		//在重新分配分区之后和消费者开始读取消息之前被调用。
		@Override
		public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
			// TODO Auto-generated method stub
			logger.debug("onPartitionsAssigned");
			for(TopicPartition topicPartition : partitions) {
				//fetch offset for each partition at the beginning
				consumer.seek(topicPartition, getOffsetFromDB(topicPartition));
			}
		}
	}
}


