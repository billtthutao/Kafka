package tk.kafka.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import tk.kafka.User;

@Component
public class DemoListener {

    private static final Logger log= LoggerFactory.getLogger(DemoListener.class);

    //声明consumerID为demo，监听topicName为topic.quick.demo的Topic
    @KafkaListener(id = "demo", topics = "test1")
    public void listen(User msgData) {
        log.info("demo receive : "+msgData);
    }
}
