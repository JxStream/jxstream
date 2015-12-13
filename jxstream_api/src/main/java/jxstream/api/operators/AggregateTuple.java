/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.api.operators;

/**
 *
 * @author brian mwambazi and andy philogene
 * @param <T>
 */
public class AggregateTuple<T> {
    
    private final  long windowStartTimeStamp;
    private final T aggregate;
    private final long tupleCount;
    
    public AggregateTuple(long windowStart,long tupleCount,T aggregate){
      this.tupleCount=tupleCount;
      this.aggregate=aggregate;
      this.windowStartTimeStamp=windowStart;
    }

    public long getWindowStartTimeStamp() {
        return windowStartTimeStamp;
    }

    public T getAggregate() {
        return aggregate;
    }

    public long getTupleCount() {
        return tupleCount;
    }
    
    @Override
    public String toString(){
        return new StringBuilder().append(tupleCount).append(":").append(windowStartTimeStamp)
                .append(":").append(aggregate).toString();
    }
    
    
   
}
