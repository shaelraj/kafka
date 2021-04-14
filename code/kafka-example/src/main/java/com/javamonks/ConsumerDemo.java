/**
 * 
 */
package com.javamonks;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author shaelraj
 *
 */
public class ConsumerDemo {

	private static final Logger LOG = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		// create prop and config
		Properties prop = new Properties();
		prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"); // bootstrap.servers
		prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // key.deserializer
		prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // value.deserializer
		prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "My_Fourth_group");
		prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // earliest/latest/none

		// create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);

		// subscribe consumer

		consumer.subscribe(Arrays.asList("first_topic"));

		// poll for new data
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

			for (ConsumerRecord<String, String> record : records) {
				LOG.info("Key : {} Value :{}", record.key(), record.value());
				LOG.info("Partition : {} Offsets :{}", record.partition(), record.offset());
			}
		}
	}

}
