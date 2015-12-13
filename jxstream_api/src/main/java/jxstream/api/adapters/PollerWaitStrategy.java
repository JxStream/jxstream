/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.api.adapters;

/**
 *
 * @author brian mwambazi and andy philogene
 */
public interface PollerWaitStrategy {
    
    
    void waitForData(long sequence);
    
}
