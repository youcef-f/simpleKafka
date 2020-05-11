package kafka.org;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerApp {

	public KafkaConsumer<Integer, String> kafkaConsumer;

	/*
	 * public static void KafkaConsumerUtil(String brokerList, String topic) {
	 * Properties props = new Properties(); // Server ip: port number, cluster
	 * separated by commas props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
	 * brokerList); // props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
	 * "sc-slave1:2181"); // Consumers can specify groups with arbitrary names. Note
	 * that consumers in the same consumer group can only consume once for the same
	 * partition. props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-1"); //
	 * Whether autocommit is enabled, default true
	 * props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true); // Automatic
	 * submission interval 1s
	 * props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
	 * 
	 * props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
	 * 
	 * // key deserializes the specified class
	 * props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
	 * StringDeserializer.class.getName()); // value deserializes the specified
	 * class, paying attention to the consistency between producer and consumer, or
	 * resolving the problem
	 * props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
	 * StringDeserializer.class.getName()); // Consumer Target kafkaConsumer = new
	 * KafkaConsumer<Integer, String>(props);
	 * kafkaConsumer.subscribe(Arrays.asList(topic)); }
	 */

	
	
	public static void main(String[] args) {

		new ConsumerApp();
        
	}
	
	private String topic_name = "test3";
	private String bootstrapServer = "localhost:9092";

	
	public  ConsumerApp() {


	    // Set up client Java properties
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        // Just a user-defined string to identify the consumer group
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group-1");
        // Enable auto offset commit
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,  StringDeserializer.class.getName());
        
         KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
    
            // List of topics to subscribe to
            consumer.subscribe(Arrays.asList(topic_name));
            
            Executors.newScheduledThreadPool(1).scheduleAtFixedRate( ()-> {
            	  
            	 System.out.println("----------------------------------------------");
            	
            	 ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));
            	
                  records.forEach(record -> {
                     System.out.println("Offset = " + record.offset() + " Key    = " +record.key() + " Value  = " +  record.value());
                  });
            	 
            }, 1000, 1000,TimeUnit.MILLISECONDS);
      
	}
}