package tk.kafka;

import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit4.SpringRunner;

@SpringBootTest
@RunWith(SpringRunner.class)
public class SendTest {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Test
    public void testDemo() throws InterruptedException {
    	User user = new User(1,"billhu","123456");
        //kafkaTemplate.send("test1", "springbootkey1",user.toString());
    	kafkaTemplate.send("test1", "springbootkey1",user);
    }
}
