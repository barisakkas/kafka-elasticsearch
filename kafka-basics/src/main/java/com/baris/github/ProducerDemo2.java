package com.baris.github;

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

public class ProducerDemo2 {
	
	static Logger logger = LoggerFactory.getLogger(ProducerDemo2.class);
	
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		for (int i=0; i<10; i++) {
			String key = "key" + Integer.toString(i);
			
			logger.info("key = " + key);
			
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("my-first-topic", key, "hello-world");
			
			producer.send(record, new Callback() {
				
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					// TODO Auto-generated method stub
					if (exception == null) {
						logger.info("Received new metadata. \n"
								+ "Topic = " + metadata.topic() + " \n"
								+ "Offset = " + metadata.offset() + "\n"
								+ "Timestamp = " + metadata.timestamp());
					} else {
						logger.error("Errorr", exception);
					}
				}
			}).get();
			
		}
		
		
		
		producer.flush();
	}

}
