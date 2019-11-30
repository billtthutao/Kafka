package tk.kafka;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;
import org.json.JSONObject;

public class KafkaConsumertest2 {
	
	private final Consumer<String, String> consumer;
	private static Logger logger = Logger.getLogger(KafkaConsumertest2.class);
	
	public KafkaConsumertest2() {
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
		KafkaConsumertest2 consumer = new KafkaConsumertest2();
		consumer.receiveMsg1();
	}

	public void receiveMsg1() {
		//subscribe topic test1
		consumer.subscribe(Collections.singletonList("test1"));
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
					}
					
					JSONObject json = new JSONObject(recordMap);
					
					logger.debug(json.toString(4));
				}
				
				//commit offset
				//method 1 异步提交,不管提交是否成功，继续执行
				consumer.commitAsync();
			}
		}finally {
			//method 2 同步提交，不断重试，直到提交成功
			try {
				consumer.commitSync();
			}catch(CommitFailedException e) {
				logger.error("commit failed", e);
			}finally {
				consumer.close();
			}
		}
	}
}



