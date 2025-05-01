package com.udemy.kafkabasics;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import org.slf4j.*;

import java.util.*;

public class ProducerDemoKey {

	private static final Logger logger = LoggerFactory.getLogger(ProducerDemoKey.class.getSimpleName());

	public static void main(String[] args) {
		logger.info("I'm a Kafka Producer!");

		Properties properties = new Properties();

		// Connection to Kafka Localhost
		properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

		// Set Producer Properties
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer", StringSerializer.class.getName());

		// Create the Producer
		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

		for (int x = 0; x < 2; x++) {

			for (int i = 0; i < 10; i++) {
				String topic = "demo.udemy";
				String key = "id_" + i;
				String value = "Hello World " + i;

				// Create a Producer Record
				ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

				// Send data
				producer.send(producerRecord, new Callback() {;
					@Override
					public void onCompletion(RecordMetadata recordMetadata, Exception e) {
						if (e == null) {
							logger.info("Key: " + key + " | Partition: " + recordMetadata.partition());
						} else {
							logger.error("Error while producing", e);
						}
					}
				});
			}

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

		// Tell the producer to send all data and block until done
		producer.flush();

		// Flush and close the producer
		producer.close();
	}

}
