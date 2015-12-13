/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.api.adapters;

import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 *
 * @author andyphil
 */
public class KafkaConnConsumer implements ConsumerConnectors<String>{
	final Producer<String, String> producer;
	private final String topic;
	
	public KafkaConnConsumer(String a_topic){
		Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "0");
		props.put("producer.type" , "async");
		
		ProducerConfig config = new ProducerConfig(props);
		producer =  new Producer<>(config);
		this.topic = a_topic; // Topic will be automatically created
	}
	
	@Override
	public void accept(String t) {
		KeyedMessage<String, String> data = new KeyedMessage<>(topic, t);
		producer.send(data);
	}
	
}
