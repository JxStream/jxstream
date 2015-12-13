/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.api.adapters;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author brian and andy
 */
class BatchedRabbitMQSupplier<T> extends RabbitMQSupplier<T>{
    private final Queue<T> batchQueue;
    public BatchedRabbitMQSupplier(RabbitMQSupplier.Builder builder) throws IOException {
        super(builder);
        batchQueue=new ArrayDeque<>(builder.batchSize);
    }
    
    
    @Override
    public T get() {  
        try {
            if(batchQueue.size()==0){
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                T batchedMsg = deserialize(delivery.getBody());
                updateBatch(batchedMsg);
                lastDeliverytag = delivery.getEnvelope().getDeliveryTag();
                msgCount++;
            }
            T msg = batchQueue.poll();
            return msg;
        } catch (InterruptedException | ShutdownSignalException | ConsumerCancelledException ex) {
            Logger.getLogger(RabbitMQSupplier.class.getName()).log(Level.SEVERE, null, ex);

        } finally {
            //we batch acknowledge messages
            if (lastDeliverytag > -1 && msgCount % msgDeliveryAckSize == 0) {
                try {
                    getChannel().basicAck(lastDeliverytag, true);
                    lastDeliverytag=-1;
                } catch (IOException ex) {
                    Logger.getLogger(RabbitMQSupplier.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        return null;
    }
    
    private void updateBatch(T msg) {
            String batchMsg=(String)msg;
            String[] mesgs=batchMsg.split("a");
            for(String s:mesgs)
                batchQueue.add((T)s);
    }
    
} 

  
