package com.mytest.springkafkademo;

import com.mytest.springkafkademo.consumer.Receiver;
import com.mytest.springkafkademo.producer.Sender;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringKafkaDemoApplicationTests {

	private final static String HELLOWORLD_TOPIC = "helloworld.t";

	@Autowired
	private Receiver receiver;

	@Autowired
	private Sender sender;

	@Test
	public void testReceive() throws Exception {
		sender.send(HELLOWORLD_TOPIC, "Hello Spring Kafka!");
		receiver.getLatch().await(10000, TimeUnit.MILLISECONDS);
		assertThat(receiver.getLatch().getCount()).isEqualTo(0);
		System.out.println("Kafka Receiver: " + receiver.getLatch().getCount());
	}

}
