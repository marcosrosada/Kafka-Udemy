package com.udemy.kafkabasics;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {

	private static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

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
		producer.send(producerRecord);

		// Tell the producer to send all data and block until done
		producer.flush();

		// Flush and close the producer
		producer.close();
	}

}
