/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.api.adapters;

/**
 *
 * @author brian mwambazi and andy philogene
 * @param <T>
 */
public interface ConsumerConnectorsFactory <T> {
    
    /**
     * Creates and returns a ConsumerConnectors Object
     * @return a new ConsumerConnectors Object
     */
    ConsumerConnectors<T> createNewConsumer();
}
