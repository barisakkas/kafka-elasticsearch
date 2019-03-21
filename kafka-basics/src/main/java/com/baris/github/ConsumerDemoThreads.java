package com.baris.github;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoThreads {
	
	static Logger logger = LoggerFactory.getLogger(ConsumerDemoThreads.class);
	
	public static void main(String[] args) {
		
		
		new ConsumerDemoThreads().run();
		//Collections.singleton(o)
		
		
		
		
	}
	
	public void run() {
		
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-group-id");
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		
		CountDownLatch latch = new CountDownLatch(1);
		
		Runnable myRunnable = new ConsumerThread(latch, properties);
		
		Thread myThread = new Thread(myRunnable);
		
		myThread.start();
		
		Runtime.getRuntime().addShutdownHook(new Thread( () -> {
			logger.info("Caught shotdown hook");
			((ConsumerThread) myRunnable).shutdown();
			try {
				latch.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			logger.info("App has exited");
		}
		));
		
		
		try {
			latch.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			logger.info("Application is closing");
		}
	}
	
	public class ConsumerThread implements Runnable {
		
		private CountDownLatch latch;
		private KafkaConsumer<String, String> consumer;
		private Logger logger = LoggerFactory.getLogger(ConsumerThread.class);
		
		public ConsumerThread(CountDownLatch latch, Properties properties) {
			this.latch = latch;
			this.consumer = new KafkaConsumer<String, String>(properties);
			this.consumer.subscribe(Arrays.asList("my-first-topic"));
		}
		
		public void shutdown() {
			consumer.wakeup();
		}

		public void run() {
			// TODO Auto-generated method stub
			try {
				while (true) {
					ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(100));
					
					for (ConsumerRecord<String, String> record : records) {
						logger.info("key :" + record.key() + "/n Value:" + record.value()
						+ "\n Partition: " + record.partition() + " \n Offset: " + record.offset());
					}
				}
			}catch (WakeupException e) {
				logger.info("Received Exception");
			} finally {
				consumer.close();
				latch.countDown();
			}
			
		}
	}

}
