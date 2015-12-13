/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.api.adapters;

import java.util.function.Supplier;
/**
 *
 * @author Andy Philogene & Brian Mwambazi
 * @param <T>
 */
public interface SupplierConnectors<T> extends Supplier<T>{
    
    /**
     *
     * @return 
     */
    default SupplierConnectors<T> getCopyWithFullMsgView(){
        throw  new UnsupportedOperationException("Not supported yet. This supplier currently does not support this operation");
    }
    
    /**
     *
     * @return true if this SupplierConnector provides a method for creating a copy with a full view of the source data. 
     * this SupplierConnector and the new copy will both see and consume the same messages {@link #getCopyWithFullMsgView}
     */
    default boolean supportsCopyWithFullMsgView(){
        return false;
    }
}
