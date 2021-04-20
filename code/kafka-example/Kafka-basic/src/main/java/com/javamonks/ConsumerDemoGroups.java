/**
 * 
 */
package com.javamonks;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author shaelraj
 * 
 * 
 *
 */

/*
 * [main] INFO org.apache.kafka.clients.consumer.internals.AbstractCoordinator -
 * [Consumer clientId=consumer-My_Fifth_group-1, groupId=My_Fifth_group] Attempt
 * to heartbeat failed since group is rebalancing [main] INFO
 * org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer
 * clientId=consumer-My_Fifth_group-1, groupId=My_Fifth_group] Revoke previously
 * assigned partitions first_topic-0, first_topic-1, first_topic-2 [main] INFO
 * org.apache.kafka.clients.consumer.internals.AbstractCoordinator - [Consumer
 * clientId=consumer-My_Fifth_group-1, groupId=My_Fifth_group] (Re-)joining
 * group [main] INFO
 * org.apache.kafka.clients.consumer.internals.AbstractCoordinator - [Consumer
 * clientId=consumer-My_Fifth_group-1, groupId=My_Fifth_group] Successfully
 * joined group with generation Generation{generationId=2,
 * memberId='consumer-My_Fifth_group-1-79540000-f1b6-4410-9a49-21b07a4fbcef',
 * protocol='range'} [main] INFO
 * org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer
 * clientId=consumer-My_Fifth_group-1, groupId=My_Fifth_group] Finished
 * assignment for group at generation 2:
 * {consumer-My_Fifth_group-1-79540000-f1b6-4410-9a49-21b07a4fbcef=Assignment(
 * partitions=[first_topic-2]),
 * consumer-My_Fifth_group-1-19923821-13c1-499c-a02f-3c22150df4a7=Assignment(
 * partitions=[first_topic-0, first_topic-1])} [main] INFO
 * org.apache.kafka.clients.consumer.internals.AbstractCoordinator - [Consumer
 * clientId=consumer-My_Fifth_group-1, groupId=My_Fifth_group] Successfully
 * synced group in generation Generation{generationId=2,
 * memberId='consumer-My_Fifth_group-1-79540000-f1b6-4410-9a49-21b07a4fbcef',
 * protocol='range'} [main] INFO
 * org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer
 * clientId=consumer-My_Fifth_group-1, groupId=My_Fifth_group] Notifying
 * assignor about the new Assignment(partitions=[first_topic-2]) [main] INFO
 * org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer
 * clientId=consumer-My_Fifth_group-1, groupId=My_Fifth_group] Adding newly
 * assigned partitions: first_topic-2 [main] INFO
 * org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer
 * clientId=consumer-My_Fifth_group-1, groupId=My_Fifth_group] Setting offset
 * for partition first_topic-2 to the committed offset FetchPosition{offset=22,
 * offsetEpoch=Optional[0],
 * currentLeader=LeaderAndEpoch{leader=Optional[DESKTOP-Q00SICC:9092 (id: 0
 * rack: null)], epoch=0}}
 */

public class ConsumerDemoGroups {

	private static final Logger LOG = LoggerFactory.getLogger(ConsumerDemoGroups.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		// create prop and config
		Properties prop = new Properties();
		prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"); // bootstrap.servers
		prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // key.deserializer
		prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // value.deserializer
		prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "My_Fifth_group");
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
