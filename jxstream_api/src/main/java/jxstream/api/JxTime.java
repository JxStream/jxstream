/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jxstream.api;

/**
 *
 * @author Brian Mwambazi & Andy Philogene
 */
public interface JxTime {

    final int Second = 1000; // Number of milliseconds in a Second
    final int Minute = 60 * JxTime.Second;
    final int Hour = 60 * JxTime.Minute;
    final int Day = 24 * JxTime.Hour;
    final int Week = 7 * JxTime.Day;

}
