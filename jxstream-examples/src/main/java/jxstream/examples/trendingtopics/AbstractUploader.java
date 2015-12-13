/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.examples.trendingtopics;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;
import java.util.function.Consumer;
import java.util.function.Function;
import jxstream.api.adapters.ConsumerConnectors;
import jxstream.api.adapters.KafkaConsumer;
import jxstream.api.adapters.RabbitMQConsumer;

/**
 *
 * @author Andy Philogene 
 */
public abstract class AbstractUploader<T> {
    private String uploadQue;
    private long msgToUpload;
    private int batchSize = 1;
    private int broker = 0; // 0 for Kafka, 1 for RabbitMQ
    private BufferedReader reader;
    Function<T, byte[]> serializer;
    
    ConsumerConnectors<T> getConsumer() throws IOException{
        ConsumerConnectors<T> con = null;
        switch(broker){
            case 0: // Kafka
                con = new KafkaConsumer.Builder(uploadQue, serializer).batchMsg(batchSize).build();
                System.out.println("Writing to Kafka");
                break;
            case 1: // RabbitMQ
                con=new RabbitMQConsumer.Builder("localhost", uploadQue, serializer).durable(false).build();
                System.out.println("Writing to rabbitmq");
                break;
        }
        return con;
    }
    
    abstract void writeDatatoQueue(ConsumerConnectors<T> con);
    
    void runInteractive() throws FileNotFoundException {
        Scanner input = new Scanner(System.in);
        System.out.println("Do you want to push data to the queue? Y/N ?");
        //int tuple_n = 0;
        String push = input.nextLine();
        String rtPath = null;
        if (push.toLowerCase().trim().equals("y")) {
            System.out.println("Queue name: ");
            uploadQue = input.nextLine();
            System.out.println("File path: ");
            rtPath = input.nextLine();
            System.out.println("Max # of messages to upload: ");
            msgToUpload = Long.parseLong(input.nextLine());
            System.out.println("Batch size for messages (1 = no batching): ");
            batchSize = Integer.parseInt(input.nextLine());
            System.out.println("Broker ID (0 for Kafka, 1 for RabbitMQ): ");
            broker = Integer.parseInt(input.nextLine());
        }
        
        FileReader file = new FileReader(rtPath);
        reader = new BufferedReader(file);
    }
}
