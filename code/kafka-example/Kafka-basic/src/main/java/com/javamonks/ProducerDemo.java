/**
 * 
 */
package com.javamonks;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author shaelraj
 *
 */
public class ProducerDemo {

	/**
	 * @param args
	 * 
	 *             Before running start zookeeper and kafka server
	 *             zookeeper-server-start config/zookeeper.properties
	 *             kafka-server-start config/server.properties
	 */
	public static void main(String[] args) {
		System.out.println("Hello World!!!");

		// Step-1: create properties
		// https://kafka.apache.org/documentation/#producerconfigs
		Properties prop = new Properties();
		prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"); // bootstrap.servers
		prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // key.serializer
		prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // value.serializer

		// Step-2: create Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

		// It will take ProducerRecord.
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello World!!!");

		// Step-3: send data
		producer.send(record);
		// it sending data asynch
		// flush producer
		producer.flush();
		// flush and close producer
		producer.close();

	}

}
