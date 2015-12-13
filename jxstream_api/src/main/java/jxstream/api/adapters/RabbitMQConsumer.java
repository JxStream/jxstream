/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.api.adapters;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import joptsimple.internal.Strings;

/**
 * This is a Java Stream consumer in that it consumes from Streams and inserts
 * into RabbitMq
 *
 * @author brimzi
 * @param <T>
 */
public class RabbitMQConsumer<T> implements ConsumerConnectors<T> {

    private final ConnectionFactory factory;
    private final Connection conn;
    private final Channel channel;
    private  String queue;
    private final Function<T, byte[]> serializer;

    private RabbitMQConsumer(RabbitMQConsumer.Builder builder) throws IOException {
        this.queue = builder.queue;
        this.factory = builder.factory;
        this.serializer = builder.serializer;
        factory.setHost(builder.host);

        if (builder.executorService != null && builder.addrs != null) {
            conn = factory.newConnection(builder.executorService, builder.addrs);
        } else if (builder.executorService != null) {
            conn = factory.newConnection(builder.executorService);
        } else if (builder.addrs != null) {
            conn = factory.newConnection(builder.addrs);
        } else {
            conn = factory.newConnection();
        }

        if (builder.channelNumber > -1) {
            channel = conn.createChannel(builder.channelNumber);
        } else {
            channel = conn.createChannel();
        }

        
        if(!Strings.isNullOrEmpty(queue))
            channel.queueDeclare(queue, builder.durable, builder.exclusive, builder.autoDelete, builder.arguments);
        else{
            AMQP.Queue.DeclareOk d=channel.queueDeclare();
            queue=d.getQueue();
        }
        
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                try {
                    getChannel().close();
                    conn.close();
                } catch (IOException ex) {
                    Logger.getLogger(RabbitMQSupplier.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        });

    }

    @Override
    public void accept(T t) {
        byte[] bytes = serialize(t);
        try {
            getChannel().basicPublish("", queue, null, bytes);
        } catch (IOException ex) {
            Logger.getLogger(RabbitMQConsumer.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    private byte[] serialize(T t) {
        return serializer.apply(t);
    }

    /**
     * @return the RabbitMQ channel used by this consumer
     */
    public Channel getChannel() {
        return channel;
    }
    
    public String getQueueName(){
        return this.queue;
    }

    public static class Builder<T> {

        private ConnectionFactory factory;
        private Function<T, byte[]> serializer;
        private final String queue;
        private final String host;
        private int channelNumber;
        private Address[] addrs;
        private ExecutorService executorService;
        private boolean durable;
        private boolean autoDelete;
        private boolean exclusive;
        private Map<String, Object> arguments;

        /**
         *
         * @param host
         * @param queue
         * @param serializer
         */
        public Builder(String host, String queue, Function<T, byte[]> serializer) {
            this.queue = queue;
            this.host = host;
            channelNumber = -1;
            this.serializer = Objects.requireNonNull(serializer, "Serializer cannnot be null");

        }

        public Builder<T> connectionFactory(ConnectionFactory factory) {
            this.factory = factory;
            return this;

        }

        public Builder<T> channel(int channelNumber) {
            this.channelNumber = channelNumber;
            return this;
        }

        public Builder<T> brokerAddresses(Address[] addrs) {
            this.addrs = addrs;
            return this;
        }

        public Builder<T> executorService(ExecutorService service) {
            this.executorService = service;
            return this;
        }

        public Builder<T> durable(boolean durable) {
            this.durable = durable;
            return this;
        }

        public Builder<T> autoDelete(boolean auto) {
            this.autoDelete = auto;
            return this;
        }

        public Builder<T> exclusive(boolean exclusive) {
            this.exclusive = exclusive;
            return this;
        }

        public Builder<T> queueArguments(Map<String, Object> args) {
            this.arguments = args;
            return this;
        }

        public<T> RabbitMQConsumer<T> build() throws IOException {
            if (factory == null) {
                factory = new ConnectionFactory();
            }

            return new RabbitMQConsumer<>(this);
        }
    }

}
