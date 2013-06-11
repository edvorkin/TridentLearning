package com.edvorkin.trident;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Created with IntelliJ IDEA.
 * User: edvorkin
 * Date: 6/10/13
 * Time: 7:55 PM
 * To change this template use File | Settings | File Templates.
 */

public  class UpperCaseFunction extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        collector.emit(new Values(tuple.getString(0).toUpperCase()));
    }
}
