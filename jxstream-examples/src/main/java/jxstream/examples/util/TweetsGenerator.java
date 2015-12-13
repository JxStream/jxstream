/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.examples.util;

import java.util.Random;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import jxstream.api.adapters.KafkaConsumer;
import jxstream.examples.trendingtopics.MyTweets;

/**
 *
 * @author Brian Mwambazi & Andy Philogene
 */
public class TweetsGenerator{
    
    public static void main(String[] args) {
        final String[] words = new String[] {"JxStream", "Storm", "Apache", "Hadoop", "Kafka", "RabbitMQ",
            "ZeroMQ", "Stream", "Java", "Kardashian", "Music", "Cat", "Summer", "Sweden"};
        final Random rand = new Random();
        Scanner input = new Scanner(System.in);
        System.out.println("Queue name: ");
        String topic = input.nextLine();
        System.out.println("Duration (Seconds): ");
        int duration = Integer.parseInt(input.nextLine());
        KafkaConsumer<MyTweets> consumer = new KafkaConsumer.Builder<>(topic, MyTweets.getSerializer()).batchMsg(50).build();
        
        int interval = 100;
        long start = System.currentTimeMillis();
        long stop = start;
        long tick = start, tack;
        while( stop-start < duration*1000 ){
            tack = System.currentTimeMillis();
            stop = tack;
            if(tack - tick > interval){
                final String word = words[rand.nextInt(words.length)];
                MyTweets tweet = new MyTweets(word, tack);
                consumer.accept(tweet);
                //System.out.println(tweet.topic +" ["+tweet.timestamp+"]");
                tick = System.currentTimeMillis();
            }
        }
        System.out.println("Done uploading tweets");
        consumer.shutdown();
    }

    
}
