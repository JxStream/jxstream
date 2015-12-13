/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.api.adapters;

/**
 *
 * @author brian mwambazi and andy philogene
 * @param <T> type of tuples the SupplierConnector will be supplying
 */
public interface SupplierConnectorsFactory<T> {
    
    /**
     * Creates and returns a SupplierConnectors Object
     * @return a new SupplierConnectors Object
     */
    SupplierConnectors<T> createNewSupplier();
    
}
