/**
 * 
 */
package com.javamonks;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author shaelraj
 *
 */
public class ProducerDemoWithCallback {

	private static final Logger LOG = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Properties prop = new Properties();
		prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"); // bootstrap.servers
		prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // key.serializer
		prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // value.serializer

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

		for (int i = 0; i < 10; i++) {
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello World!!!" + Integer.toString(i));

			producer.send(record, new Callback() {

				public void onCompletion(RecordMetadata metadata, Exception e) {
					// Execute every time when record sent successfully or exception thrown
					if (e != null) {
						LOG.error("Error occured : {}", e.getMessage());
					}

					LOG.info("Received Metadata \n Topic:{} \n Partition :{} \n Offset :{} \n TimeStamp :{}",
							metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());

				}
			});
		}
		producer.flush();
		producer.close();
	}

}
