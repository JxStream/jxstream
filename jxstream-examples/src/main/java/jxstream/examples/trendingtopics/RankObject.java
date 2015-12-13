/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.examples.trendingtopics;

/**
 * Object used to store a rankable tweet
 * @author Brian Mwambazi & Andy Philogene
 */
public class RankObject implements Comparable<RankObject> {

    public final String topic;
    public final int count;

    /**
     *
     * @param topic
     * @param count
     */
    public RankObject(String topic, int count) {
        this.topic = topic;
        this.count = count;
    }

    @Override
    public int compareTo(RankObject other) {
        long delta = this.count - other.count;
        if (delta > 0) {
            return -1;
        } else if (delta < 0) {
            return 1;
        } else {
            return 0;
        }
    }
    
    @Override
    public String toString(){
        return this.topic + " ["+this.count +"]";
    }
}
