package com.udemy.kafkabasics;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import org.slf4j.*;

import java.util.*;

public class ProducerDemoWithCallback {

	private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

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

		// Create a Producer Record
		ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo.udemy", "Hello World");

		// Send data
		producer.send(producerRecord, new Callback() {;
			@Override
			public void onCompletion(RecordMetadata recordMetadata, Exception e) {
				if (e == null) {
					logger.info("Received new metadata. \n" +
							"Topic: " + recordMetadata.topic() + "\n" +
							"Partition: " + recordMetadata.partition() + "\n" +
							"Offset: " + recordMetadata.offset() + "\n" +
							"Timestamp: " + recordMetadata.timestamp());
				} else {
					logger.error("Error while producing", e);
				}
			}
		});

		// Tell the producer to send all data and block until done
		producer.flush();

		// Flush and close the producer
		producer.close();
	}

}
