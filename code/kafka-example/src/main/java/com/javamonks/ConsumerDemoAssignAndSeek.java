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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author shaelraj
 *
 */
public class ConsumerDemoAssignAndSeek {

	private static final Logger LOG = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		String bootStrapSever = "127.0.0.1:9092";
		String topic = "first_topic";

		// create prop and config
		Properties prop = new Properties();
		prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapSever); // bootstrap.servers
		prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // key.deserializer
		prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // value.deserializer
		prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // earliest/latest/none

		// create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);

		// assign
		TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
		consumer.assign(Arrays.asList(partitionToReadFrom));

		long offsetToRead = 10L;

		// seek consumer
		consumer.seek(partitionToReadFrom, offsetToRead);

		int noOfMsgToRead = 5;
		boolean keepReading = true;
		int count = 0;

		// poll for new data
		while (keepReading) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

			for (ConsumerRecord<String, String> record : records) {
				count++;
				LOG.info("Key : {} Value :{}", record.key(), record.value());
				LOG.info("Partition : {} Offsets :{}", record.partition(), record.offset());
				if (count >= noOfMsgToRead) {
					keepReading = false;
					break;
				}
			}
		}
		consumer.close();
		LOG.info("Application existng !!!");
	}

}
