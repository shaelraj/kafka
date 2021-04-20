/**
 * 
 */
package com.javamonks;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * @author shaelraj
 *
 */
public class ConsumerDemoWithThread {

	private static final Logger LOG = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new ConsumerDemoWithThread().run();
	}

	private void run() {
		String bootStrapSever = "127.0.0.1:9092";
		String groupId = "My_Sixth_group";
		String topic = "first_topic";
		CountDownLatch latch = new CountDownLatch(1);
		LOG.info("creating consumer thread");
		Runnable runnable = new ConsumerRunnable(bootStrapSever, groupId, topic, latch);

		Thread thread = new Thread(runnable);
		thread.start();

		// add shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			LOG.info("Caught shutdown hook");
			((ConsumerRunnable) runnable).shutdown();
			try {
				latch.await();
			} catch (InterruptedException e) {
				LOG.error("Got interrupted exception :{}", e.getMessage());
			} finally {
				LOG.info("Application shutdown finally!!!");
			}
		}));

		try {
			latch.await();
		} catch (InterruptedException e) {
			LOG.error("Got interrupted exception :{}", e.getMessage());
		} finally {
			LOG.info("Application shutdown finally!!!");
		}
	}

	public class ConsumerRunnable implements Runnable {

		private CountDownLatch latch;

		private KafkaConsumer<String, String> consumer;

		public ConsumerRunnable(String bootStrapServer, String groupId, String topic, CountDownLatch latch) {
			this.latch = latch;
			Properties prop = new Properties();
			prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer); // bootstrap.servers
			prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // key.deserializer
			prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // value.deserializer
			prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // earliest/latest/none
			consumer = new KafkaConsumer<String, String>(prop);

			consumer.subscribe(Arrays.asList(topic));
		}

		public void run() {
			try {
				while (true) {
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

					for (ConsumerRecord<String, String> record : records) {
						LOG.info("Key : {} Value :{}", record.key(), record.value());
						LOG.info("Partition : {} Offsets :{}", record.partition(), record.offset());
					}
				}
			} catch (WakeupException e) {
				LOG.info("Received Shutdown signal!!!");
			} finally {
				consumer.close();
				// tell our main code we are done with consumer
				latch.countDown();
			}

		}

		public void shutdown() {
			// the wakup method is special method to interupt consumer poll
			// it will throw exception WakeupException
			consumer.wakeup();
		}

	}

}
