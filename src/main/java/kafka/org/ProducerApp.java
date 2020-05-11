package kafka.org;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerApp {

	private String bootstrapServer = "localhost:9092";
	private String topic_name = "test3";
	private String client_id = "client_prod_1";
	private int counter = 0;

	public static void main(String[] args) {
		new ProducerApp();
	}

	// constructor
	public ProducerApp() {
		Properties properties = new Properties();

		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.put(ProducerConfig.CLIENT_ID_CONFIG, client_id);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> {

			++counter;
			String message = String.valueOf(Math.random() * 1000);

			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic_name, String.valueOf(counter), message);

			producer.send(record,  new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if(e != null) {
                       e.printStackTrace();
                    } else {
                      
                    	System.out.println(" offset: " + metadata.offset()+ " partition: " + metadata.partition() + " key: " + counter + " value: " + message);
                    }
                }
            });

		}, 1000, 100, TimeUnit.MILLISECONDS);

		// producer.flush();
		// producer.close();
	}

}
