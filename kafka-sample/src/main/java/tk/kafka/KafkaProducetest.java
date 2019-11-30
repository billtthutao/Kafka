package tk.kafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaProducetest {
	
	private final Producer<String, String> producer;
    
    public KafkaProducetest() {
    
    	// TODO Auto-generated method stub
    	Properties kafkaProps = new Properties();
    	//设置broker
    	kafkaProps.put("bootstrap.servers", "kafka1:9092,kafka2:9093,kafka3:9094");
    	//设置自定义分区器
    	kafkaProps.put("partitioner.class", "tk.kafka.MyPartitioner");
    	//设置序列化
    	kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    	kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    	//其他
    	kafkaProps.put("acks", "all");		
    	
    	producer = new KafkaProducer<String, String>(kafkaProps);
    }
    
	public static void main(String[] args) {
		
		KafkaProducetest producer = new KafkaProducetest();
		producer.sendMsg1();
		producer.sendMsg2();
	}

	public void sendMsg1() {
		ProducerRecord<String, String> record = new ProducerRecord<>("test1","ProductStart", "IPhone 12");
		
		producer.send(record);
		
	}
	
	public void sendMsg2() {
		ProducerRecord<String, String> record = new ProducerRecord<>("test1","Precision Products", "XiaoMi");
		
		try {
			producer.send(record, new DemoCallback()).get();
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

//callback function
class DemoCallback implements Callback{

	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		// TODO Auto-generated method stub
		if(exception != null) {
			exception.printStackTrace();
		}else {
			System.out.println(metadata.topic());
			System.out.println(metadata);
		}
	}
	
}

