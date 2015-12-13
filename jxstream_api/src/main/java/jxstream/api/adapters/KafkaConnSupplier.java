/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.api.adapters;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 *
 * @author andyphil
 */


public class KafkaConnSupplier implements SupplierConnectors<String>{
	private final ConsumerIterator<byte[], byte[]> it;
	
	public KafkaConnSupplier(String a_zookeeper, String a_groupId, String a_topic){
		ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig(a_zookeeper, a_groupId));
		Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(a_topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(a_topic);
		KafkaStream stream = streams.get(0);
		it = stream.iterator();
	}
	
	private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
		props.put("autooffset.reset", "smallest");
		
        return new ConsumerConfig(props);
    }
	
	@Override
	public String get(){
		
		if(it.hasNext()){
			return new String( it.next().message());
		}
		return null;
	}
}
