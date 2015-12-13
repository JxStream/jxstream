
package jxstream.examples.trendingtopics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.function.BiFunction;
import java.util.function.Function;
import jxstream.api.JxStream;
import jxstream.api.JxTime;
import jxstream.api.adapters.KafkaSupplier;
import jxstream.api.adapters.SupplierConnectors;
import jxstream.api.operators.AggregateTuple;
import jxstream.api.operators.Window;

/**
 *
 * @author Brian Mwambazi & Andy Philogene
 */
public class TrendingTopics {
    
    public static void main(String[] args) throws IOException{
        Scanner input = new Scanner(System.in);
        String topic, groupID;
        if(args.length > 1){
            topic = args[0];
            groupID = args[1];
        }else{
            System.out.println("Queue name: ");
            topic = input.nextLine();
            System.out.println("groupID: ");
            groupID = input.nextLine();
        }
        
        KafkaSupplier<MyTweets> sup = new KafkaSupplier.Builder(topic, groupID, MyTweets.getDeserializer()).build();
        
        processData(sup);
    }
    
    private static void processData(SupplierConnectors sup) throws IOException{
        
        Integer TopN = 5;
        Window win = Window.createTimeBasedWindow( JxTime.Minute, // Size
                                JxTime.Minute, // Advance
                                null, // Default value
                                (MyTweets x)-> x.timestamp); // timestamp retrieval
        
        /**  Operator Lambda to apply to each incoming tuple ****/
        BiFunction< Map<String,Integer>, MyTweets, Map<String,Integer> > operator = (map, newobj) -> {
            if(map == null){
                map = new HashMap<>();
            }else{
                if(map.containsKey(newobj.topic)){
                    map.put(newobj.topic, map.get(newobj.topic)+1);
                }else{
                    map.put(newobj.topic, 1);
                }
            }
            return map;    
        };
        /** Finisher Lambda to aggregate the result when a window expires  ***/
        Function< Map<String,Integer>, ArrayList<RankObject> > finisher = map ->{
            Integer i = 0;
            ArrayList<RankObject> rankings = new ArrayList<>();
            map.entrySet().forEach(e ->{
                rankings.add( new RankObject(e.getKey(), e.getValue()));
            });
            Collections.sort(rankings);
            Iterator<RankObject> it = rankings.iterator();
            while(it.hasNext()){
                RankObject o = it.next();
                if(++i > TopN){
                    it.remove();
                }
            }
            map.clear();
            return rankings;
        };
        
        /**  Stream computation logic  ***/
        JxStream<MyTweets> jstream = JxStream.generate(sup);
        jstream //.peek(x->System.out.println("Read: " + x.topic +"["+x.timestamp+"]"))
            .Aggregate(operator, finisher, win)
            .forEach( v->{
                AggregateTuple<ArrayList<RankObject>  > agg = (AggregateTuple<ArrayList<RankObject>  >) v;
                ArrayList<RankObject>   res =  agg.getAggregate();
                System.out.println("Trending topics: " + res.toString() );
            })
            .start();
     
                
    }
   
}
