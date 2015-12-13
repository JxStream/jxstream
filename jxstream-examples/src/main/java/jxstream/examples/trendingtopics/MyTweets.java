/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.examples.trendingtopics;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
/**
 *
 * @author Brian Mwambazi & Andy Philogene
 */
public class MyTweets implements Serializable{
    public final String topic;
    public final long timestamp;
    
    /**
     *
     * @param topic the name of the topic
     * @param stamp the timestamp of the topic
     */
    public MyTweets(String topic, long stamp) {
        this.topic = topic;
        this.timestamp = stamp;
    }
    
    /**
     * Creates a lambda expression to serialize the tweet object to byte[]
     * @return Function<MyTweets, byte[]> a lambda expression
     */
    public static Function<MyTweets, byte[]> getSerializer(){
        
        final Function<MyTweets, byte[]> serializer = t -> {
            ObjectOutputStream o = null;
            byte[] out = null;
            try {
                ByteArrayOutputStream b = new ByteArrayOutputStream();
                o = new ObjectOutputStream(b);
                o.writeObject(t);
                out =  b.toByteArray();
            } catch (IOException ex) {
                Logger.getLogger(MyTweets.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                try {
                    o.close();
                } catch (IOException ex) {
                    Logger.getLogger(MyTweets.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            return out;
        };
        return serializer;
    }

    /**
     * Creates a lambda expression to deserialize the tweet object from byte[]
     * @return Function<byte[], MyTweets> a lambda expression
     */
    public static Function<byte[], MyTweets> getDeserializer() {
        
        final Function<byte[], MyTweets> deserializer = bytes ->{
            ObjectInputStream o = null;
            MyTweets myobject = null;
            try {
                ByteArrayInputStream b = new ByteArrayInputStream(bytes);
                o = new ObjectInputStream(b);
                myobject = (MyTweets) o.readObject();
            } catch (IOException | ClassNotFoundException ex) {
                Logger.getLogger(MyTweets.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                try {
                    o.close();
                } catch (IOException ex) {
                    Logger.getLogger(MyTweets.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            
            return myobject;
        };
        return deserializer;
    }
}
