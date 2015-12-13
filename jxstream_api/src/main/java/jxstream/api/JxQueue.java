/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.api;

import jxstream.api.adapters.ConsumerConnectors;
import jxstream.api.adapters.SupplierConnectors;
import jxstream.api.adapters.SupplierConnectorsFactory;

/**
 *
 * @author brian mwambazi and andy philogene
 */
class JxQueue<T> {
    
    private final SupplierConnectorsFactory<T> factory;
    private final ConsumerConnectors<T> sink;

    JxQueue(SupplierConnectorsFactory<T> factory, ConsumerConnectors<T> sink) {
        this.factory = factory;
        this.sink = sink;
    }

    SupplierConnectorsFactory<T> getSupplierFactory() {
        return factory;
    }

    ConsumerConnectors<T> getSink() {
        return sink;
    }

    
    
    
    
    
    
}
  
