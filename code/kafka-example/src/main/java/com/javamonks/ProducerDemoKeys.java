/**
 * 
 */
package com.javamonks;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author shaelraj
 *
 */
public class ProducerDemoKeys {

	private static final Logger LOG = LoggerFactory.getLogger(ProducerDemoKeys.class);

	/**
	 * @param args
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		Properties prop = new Properties();
		prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"); // bootstrap.servers
		prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // key.serializer
		prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // value.serializer

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

		String topic = "first_topic";
		for (int i = 0; i < 10; i++) {

			String value = "Hello World " + Integer.toString(i);
			String key = "ID_" + Integer.toString(i);

			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
			LOG.info("KEY : {}", key);
			producer.send(record, new Callback() {

				public void onCompletion(RecordMetadata metadata, Exception e) {
					// Execute every time when record sent successfully or exception thrown
					if (e != null) {
						LOG.error("Error occured : {}", e.getMessage());
					}

					LOG.info("Received Metadata \n Topic:{} \n Partition :{} \n Offset :{} \n TimeStamp :{}",
							metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());

				}
			}).get(); // maked async call sync. Not recommended
		}
		producer.flush();
		producer.close();
	}

}
