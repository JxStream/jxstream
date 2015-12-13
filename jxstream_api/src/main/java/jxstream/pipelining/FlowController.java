/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.pipelining;

import java.util.function.Function;

/**
 *
 * @author brian mwambazi and andy philogene
 */
public interface FlowController<T> extends Function<T, Integer> {
	/**
     *
     * @return parts number of partitions to split the stream
     */
	 int getParts();
}
