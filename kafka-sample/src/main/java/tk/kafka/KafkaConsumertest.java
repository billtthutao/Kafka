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

public class KafkaConsumertest {
	
	private final Consumer<String, String> consumer;
	private static Logger logger = Logger.getLogger(KafkaConsumertest.class);
	private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
	
	public KafkaConsumertest() {
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
		KafkaConsumertest consumer = new KafkaConsumertest();
		consumer.receiveMsg1();
	}

	public void receiveMsg1() {
		//subscribe topic test1
		consumer.subscribe(Collections.singletonList("test1"),new HandleRebalance());
		Map<String, String> recordMap = new HashMap<>();
		
		try {
			while(true) {
				ConsumerRecords<String,String> records = consumer.poll(100);
				for(ConsumerRecord<String,String> record : records) {
					
					logger.debug(System.out.format("topic = %s, partition = %s, offset = %d, customer = %s, msg = %s\n",
								 record.topic(),record.partition(),record.offset(),record.key(),record.value()
							));
					
					if(record.key() != null) {
						recordMap.put(record.key(), record.value());
						currentOffsets.put(new TopicPartition(record.topic(),record.partition()), new OffsetAndMetadata(record.offset()+1,"no metadata"));
					}
					
					JSONObject json = new JSONObject(recordMap);
					
					logger.debug(json.toString(4));
				}
				
				//commit offset
				//method 1 异步提交,不管提交是否成功，继续执行
				consumer.commitAsync(currentOffsets, null);
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
	
	private class HandleRebalance implements ConsumerRebalanceListener{

		//会在再均衡开始之前和消费者停止读取消息之后被调用
		@Override
		public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			// TODO Auto-generated method stub
			logger.info(System.out.format("Lost partitions in rebalance. Committing current offsets:%s", currentOffsets));
			consumer.commitSync(currentOffsets);
		}

		//在重新分配分区之后和消费者开始读取消息之前被调用。
		@Override
		public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
			// TODO Auto-generated method stub
			
		}
	}
}


