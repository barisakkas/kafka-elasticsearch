package com.github.baris;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterStream {
	
	
	
	public static void main(String[] args) {
		try {
			new TwitterStream().createEndpoint();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void createEndpoint() throws InterruptedException {
		
		String consumerKey = "HRcqZVU5JWRf0VIsQFCJLfIKi";
		String consumerSecret = "jfb1NrX8Ga0yqsaatNXPLQEx6E20YQkHPa3qBiwOvQQRsvGnUB";
		String token = "2485040318-JF8saCnxrtgqZ8V1j8LUy1EJP3YfMu4PiAfm4rN";
		String secret = "yZPCOB2d5Ikl6OPyKbEFp0yffPpS2PrB65jhOrCKQpvc3";
		
		/** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
		//BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		//List<Long> followings = Lists.newArrayList(1234L, 566788L);
		List<String> terms = Lists.newArrayList("kafka");
		//hosebirdEndpoint.followings(followings);
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
		
		// Create a new BasicClient. By default gzip is enabled.
	    BasicClient client = new ClientBuilder()
	            .name("sampleExampleClient")
	            .hosts(Constants.STREAM_HOST)
	            .endpoint(hosebirdEndpoint)
	            .authentication(hosebirdAuth)
	            .processor(new StringDelimitedProcessor(msgQueue))
	            .build();
	    
	    Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		ProducerRecord<String, String> record;

	    // Establish a connection
	    client.connect();

	    // Do whatever needs to be done with messages
	    for (int msgRead = 0; msgRead < 1000; msgRead++) {
	      if (client.isDone()) {
	        System.out.println("Client connection closed unexpectedly: " + client.getExitEvent().getMessage());
	        break;
	      }

	      String msg = msgQueue.poll(5, TimeUnit.SECONDS);
	      if (msg == null) {
	        System.out.println("Did not receive a message in 5 seconds");
	      } else {
	        System.out.println(msg);
	        record = new ProducerRecord<String, String>("my-first-topic", msg);
	        producer.send(record);
	    	producer.flush();
	      }
	    }

	    client.stop();
	    producer.close();

	    // Print some stats
	    System.out.printf("The client read %d messages!\n", client.getStatsTracker().getNumMessages());
	}

}
