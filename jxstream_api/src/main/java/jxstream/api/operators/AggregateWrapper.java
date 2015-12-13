/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.api.operators;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 *
 * @author brian mwambazi and andy philogene
 * @param <T>
 * @param <R>
 */
public interface AggregateWrapper<T,R> extends Function<T,R>  {
    
}
