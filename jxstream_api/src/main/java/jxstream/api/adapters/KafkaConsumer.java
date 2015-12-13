/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.api.adapters;

import java.util.Properties;
import java.util.function.Function;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.producer.KeyedMessage;
import kafka.message.Message;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 *
 * @author Andy Philogene
 * @param <T>
 */
public class KafkaConsumer<T> implements ConsumerConnectors<T> {

    //private final KafkaProducer<String, byte[]> producer;
    private final Producer<String, byte[]> producer;
    private final String topic;
    private final Function<T, byte[]> serializer;
    
    private KafkaConsumer(Builder builder) {
        this.producer = new Producer<>(new ProducerConfig(builder.props));
        this.topic = builder.topic;
        this.serializer = builder.serializer;
    }

    @Override
    public void accept(T t) {
        KeyedMessage<String, byte[]> data = new KeyedMessage<>(topic, serializer.apply(t));
        producer.send(data);
    }

    public void shutdown() {
        producer.close();
    }

    public static class Builder<T> {

        private final String topic;
        private Properties props = new Properties();
        private final Function<T, byte[]> serializer;
        
        public Builder(String topic, Function<T, byte[]> ser) {
            this.topic = topic;
            this.serializer =  ser;
            props.put("metadata.broker.list", "localhost:9092");
            props.put("serializer.class", "kafka.serializer.DefaultEncoder");
            props.put("key.serializer.class", "kafka.serializer.StringEncoder");
            props.put("request.required.acks", "1");
            props.put("producer.type", "async");
        }

        public Builder isSync(boolean sync) {
            props.put("producer.type", (sync) ? "sync" : "async");
            return this;
        }

        public Builder addCostumProps(Properties props) {
            this.props.putAll(props);
            return this;
        }

        public Builder batchMsg(int size) {
            props.put("batch.num.messages", String.valueOf(size));
            return this;
        }

        public Builder addKafkaServer(String servers) {
            props.put("metadata.broker.list", servers);
            return this;
        }

        public KafkaConsumer build() {
            return new KafkaConsumer(this);
        }
    }
}
