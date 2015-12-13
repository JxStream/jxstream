/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.api.adapters;

import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import joptsimple.internal.Strings;

/**
 *
 * @author brian and andy
 * @param <T>
 */
public class RabbitMQSupplier<T> implements SupplierConnectors<T> {

    protected final ConnectionFactory factory;
    protected final Connection conn;
    private final Channel channel;
    protected String queue;
    protected long lastDeliverytag;
    protected int msgDeliveryAckSize = 20;
    protected QueueingConsumer consumer;
    protected final Function<byte[], T> deserializer;
    protected int msgCount;
    protected Thread shutDownHook;
    private final boolean acknowledge;

    
    protected RabbitMQSupplier(RabbitMQSupplier.Builder builder) throws IOException {
        lastDeliverytag = -1;
        this.queue = builder.queue;
        this.factory = builder.factory;
        this.deserializer = builder.deserializer;
        factory.setHost(builder.host);

        if (builder.msgDeliveryAckSize > 0) {
            this.msgDeliveryAckSize = builder.msgDeliveryAckSize;
        }
        
        this.acknowledge=builder.explicitAck;

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
            DeclareOk d=channel.queueDeclare();
            queue=d.getQueue();
        }
        
        consumer = new QueueingConsumer(getChannel());

        channel.basicConsume(queue, false, consumer);
        
        //channel.basicQos(50);
        
        shutDownHook = new Thread() {
            @Override
            public void run() {
                try {
                    if (lastDeliverytag > -1) {
                        getChannel().basicAck(lastDeliverytag, true);
                    }
                    getChannel().close();
                    conn.close();
                } catch (IOException ex) {
                    Logger.getLogger(RabbitMQSupplier.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        };
        Runtime.getRuntime().addShutdownHook(shutDownHook);
    }

    @Override
    public T get() {
        try {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            lastDeliverytag = delivery.getEnvelope().getDeliveryTag();
            msgCount++;
            
            T msg = deserialize(delivery.getBody());
            return msg;
        } catch (InterruptedException | ShutdownSignalException | ConsumerCancelledException ex) {
            Logger.getLogger(RabbitMQSupplier.class.getName()).log(Level.SEVERE, null, ex);

        } finally {
            //we batch acknowledge messages
            if (acknowledge && lastDeliverytag > -1 && msgCount % msgDeliveryAckSize == 0) {
                try {
                    getChannel().basicAck(lastDeliverytag, true);
                    lastDeliverytag = -1;
                } catch (IOException ex) {
                    Logger.getLogger(RabbitMQSupplier.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        return null;
    }

    protected T deserialize(byte[] body) {
        return deserializer.apply(body);
    }

    /**
     * @return the channel used by this suppliers
     */
    public Channel getChannel() {
        return channel;
    }

    
    public static class Builder<T> {
        private ConnectionFactory factory;
        private final String queue;
        private final String host;
        private int channelNumber;
        private Address[] addrs;
        private ExecutorService executorService;
        private boolean durable;
        private final Function<byte[], T> deserializer;
        private boolean autoDelete;
        private boolean exclusive;
        private Map<String, Object> arguments;
        private int msgDeliveryAckSize;
        protected int batchSize;
        private boolean explicitAck=true;

        /**
         *
         * @param host
         * @param queue
         * @param deserializer
         */
        public Builder(String host, String queue, Function<byte[], T> deserializer) {
            this.queue = queue;
            this.host = host;
            channelNumber = -1;
            batchSize = 1;

            this.deserializer = Objects.requireNonNull(deserializer, "deserializer cannot be null");
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

        public Builder<T> msgDeliveryAckSize(int size) {
            this.msgDeliveryAckSize = size;
            return this;
        }

        public Builder<T> batchSize(int size) {
            this.batchSize = size;
            return this;
        }
        public Builder<T> explicitAck(boolean ack) {
            this.explicitAck = ack;
            return this;
        }

        public RabbitMQSupplier<T> build() throws IOException {
            if (factory == null) {
                factory = new ConnectionFactory();
            }
            if (batchSize > 1) {
                return new BatchedRabbitMQSupplier<>(this);
            } else {
                return new RabbitMQSupplier<>(this);
            }
        }
    }

}
