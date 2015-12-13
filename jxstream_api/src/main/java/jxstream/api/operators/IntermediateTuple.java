/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.api.operators;

import java.util.ArrayList;

/**
 *
 * @author brian mwambazi and andy philogene
 */
public class IntermediateTuple {

    public Double aggregate;
    public Double state;
    public ArrayList< Double> elements;

    /**
     *
     * @param aggregate
     * @param state
     */
    public IntermediateTuple(Double aggregate, Double state) {
        this.aggregate = aggregate;
        this.state = state;
    }

    public IntermediateTuple(ArrayList< Double> elements) {
        this.elements = elements;
    }

}
