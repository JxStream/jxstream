/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.api.adapters;

import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Reads batched-messages from source and outputs normal tuples
 * @author brian and andy
 * @param <T>
 */
public class BatchMsgSupplier<T> implements SupplierConnectors<T> {
    private final SupplierConnectors<T> source;
    private final Queue<T> batchQueue;
    
    public BatchMsgSupplier(SupplierConnectors<T> source,int batchSize) {
        this.source = source;
        this.batchQueue = new ArrayDeque<>(batchSize);
    }
    

    @Override
    public T get() {
        try {
            if(batchQueue.size()==0){
                T batchedMsg = source.get();
                updateBatch(batchedMsg);
            }
            T msg = batchQueue.poll();
            return msg;
        } catch (ShutdownSignalException | ConsumerCancelledException ex) {
            Logger.getLogger(RabbitMQSupplier.class.getName()).log(Level.SEVERE, null, ex);

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
