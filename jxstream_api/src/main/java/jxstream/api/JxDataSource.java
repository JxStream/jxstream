/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.api;

import java.util.function.Supplier;
import jxstream.api.adapters.SupplierConnectors;
import jxstream.api.adapters.SupplierConnectorsFactory;

/**
 *
 * @author brian mwambazi and philogene
 * @param <T> the type of data this Queue stores
 */
public class JxDataSource<T> {
    
    private final SupplierConnectorsFactory<T> factory;
    private PipelineEngine engine;
         

    JxDataSource(SupplierConnectorsFactory<T> src) {
        this(src,new PipelineEngine());
    }
    
    JxDataSource(SupplierConnectorsFactory<T> src,PipelineEngine engine) {
        this.factory = src;
        this.engine=engine;
    }
    
    
    
    SupplierConnectors<T> getSupplier(){
        return factory.createNewSupplier();
    }

//    void setPipelineEngine(PipelineEngine engine) {
//        this.engine=engine;
//    }
    
    PipelineEngine getPipelineEngine(){
        return this.engine;
    }
    
    
}
