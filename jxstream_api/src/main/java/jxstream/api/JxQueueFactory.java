/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.api;

import java.io.IOException;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import jxstream.api.adapters.DisruptorConsumer;
import jxstream.api.adapters.DisruptorSupplier;
import jxstream.api.adapters.RabbitMQConsumer;
import jxstream.api.adapters.RabbitMQSupplier;
import jxstream.api.adapters.SupplierConnectorsFactory;
import jxstream.pipelining.Ring;

/**
 *
 * @author brian mwambazi and andy philogene
 */
 class JxQueueFactory {

    private JxQueueFactory() {
        
    }
    
    
    static <T>JxQueue<T> newTempQueue(JxQueueType type,Function<byte[],T> deserializer,Function<T,byte[]> serializer,PipelineEngine engine) throws IOException{
        JxQueue<T> rtVal=null;
        switch(type){
            case Rabbit:
                rtVal=createRabbitQ(deserializer,serializer);
                break;
            case Kafka:
                rtVal=createKafkaQ();
                break;
            case Disruptor:
                rtVal=createDisruptorQ(engine);
                break;
        }
        
        return rtVal;
        
    }

    private static <T> JxQueue<T> createRabbitQ(Function<byte[],T> deserializer,Function<T,byte[]> serializer) throws IOException {
        
        
        RabbitMQSupplier<T> sup = new RabbitMQSupplier.Builder<>("localhost", "",
                deserializer).build();
        RabbitMQConsumer<T> con=new RabbitMQConsumer.Builder<>("localhost", "",
                        serializer).build();
        
        final String que=con.getQueueName();
        SupplierConnectorsFactory factory=()->{
            try {
                return new RabbitMQSupplier.Builder<>("localhost", que,
                        deserializer).build();
            } catch (IOException ex) {
                Logger.getLogger(JxQueueFactory.class.getName()).log(Level.SEVERE, null, ex);
            }
            return null;
        };
        
        return new JxQueue<>(factory,con);
        
    }

    private static <T> JxQueue<T> createKafkaQ() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private static <T> JxQueue<T> createDisruptorQ(PipelineEngine engine) throws IOException {

        final Ring<T> ring=new Ring<>(1024);
        engine.addSwitch(()->{
            ring.start();
        });
        DisruptorConsumer con =new DisruptorConsumer<>(ring.getDisruptor().getRingBuffer());
        SupplierConnectorsFactory factory=()->{
            DisruptorSupplier sup=null;
            try {
                 sup =new DisruptorSupplier<>(ring.getDisruptor());
            } catch (IOException ex) {
                Logger.getLogger(JxQueueFactory.class.getName()).log(Level.SEVERE, null, ex);
            }
            
            return sup;
        };
        return  new JxQueue<>(factory,con);
    }
    
    
}
