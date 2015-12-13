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
import java.util.function.Function;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * @param <T>
 * @author Brian Mwambazi and Andy Philogene
 */
public class KafkaSupplier<T> implements SupplierConnectors<T> {

    private final ConsumerConnector consumer;
    private final ConsumerIterator<byte[], byte[]> it;
    private final Function<byte[], T> deserializer;

    private KafkaSupplier(Builder builder) {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                new ConsumerConfig(builder.props));
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(builder.topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(builder.topic);
        KafkaStream stream = streams.get(0);
        it = stream.iterator();
        deserializer = builder.deserializer;
    }

    @Override
    public T get() {
        if (it.hasNext()) {
            return deserializer.apply( it.next().message() );
        }
        return null;
    }

    public void shutdown() {
        consumer.shutdown();
    }

    public static class Builder<T> {

        private final String topic;
        private Properties props = new Properties();
        Function<byte[], T> deserializer;

        public Builder(String topic, String group, Function<byte[], T> deserializer) {
            this.topic = topic;
            this.deserializer = deserializer;
            props.put("group.id", group);
            props.put("zookeeper.connect", "127.0.0.1:2181");
            props.put("zookeeper.session.timeout.ms", "400");
            props.put("zookeeper.sync.time.ms", "200");
            props.put("auto.commit.enable", "true");
            props.put("auto.commit.interval.ms", "1000");
            props.put("auto.offset.reset", "smallest");

        }

        public Builder addCostumProps(Properties props) {
            this.props.putAll(props);
            return this;
        }
        
        public Builder batchMsg(int size){
            props.put("queued.max.message.chunks", String.valueOf(size));
            return this;
        }
        public Builder ZkServers(String servers) {
            this.props.put("zookeeper.connect", servers);
            return this;
        }

        public KafkaSupplier build() {
            return new KafkaSupplier(this);
        }
    }
}
