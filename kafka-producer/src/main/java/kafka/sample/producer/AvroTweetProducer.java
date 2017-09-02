package kafka.sample.producer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;

import ch.trivadis.sample.twitter.avro.v1.TwitterStatusUpdate;

public class AvroTweetProducer  {
	private Producer<String, TwitterStatusUpdate> producer = null;
	private String kafkaTopicTweetStatus = "tweet";

	private Producer<String, TwitterStatusUpdate> connect() {
		Producer<String, TwitterStatusUpdate> producer = null;
    	
		Properties props = new Properties();
	    props.put("bootstrap.servers", "192.168.142.128:9092");
	    props.put("acks", "all");
	    props.put("retries", 0);
	    props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
	    props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
	    props.put("schema.registry.url", "http://192.168.142.128:8081");

		try {
    		producer = new KafkaProducer<String, TwitterStatusUpdate>(props);
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    	return producer;
	}
	
	public void produce(TwitterStatusUpdate status) {
        final Random rnd = new Random();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<TwitterStatusUpdate> writer = new SpecificDatumWriter<TwitterStatusUpdate>(TwitterStatusUpdate.class);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        try {
			writer.write(status, encoder);
	        encoder.flush();
	        out.close();
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
        
        if (producer == null) {
        	producer = connect();
        }
        
        Integer key = rnd.nextInt(255);
        
        ProducerRecord<String, TwitterStatusUpdate> record = new ProducerRecord<String, TwitterStatusUpdate>(kafkaTopicTweetStatus, null, status);

        if (producer !=null) {
        	try {
//            	System.out.println("Sending to Kafka " + status.getCreatedAt() + "/" + status.getTweetId());
                producer.send(record);
        	} catch (Exception e) {
        		e.printStackTrace();
        	}
        	
        }
		
	}

}
