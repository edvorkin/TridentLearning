package com.edvorkin.trident;

import backtype.storm.tuple.Values;
import org.apache.commons.collections.MapUtils;
import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: edvorkin
 * Date: 6/10/13
 * Time: 8:17 PM
 * To change this template use File | Settings | File Templates.
 */
public class LocalAggregator implements Aggregator<Map<String, Integer>>
{
    int partitionId;
    @Override
    public Map<String, Integer> init(Object o, TridentCollector tridentCollector) {
        return new HashMap<String, Integer>();
    }

    @Override
    public void aggregate(Map<String, Integer> stringIntegerMap, TridentTuple tuple, TridentCollector tridentCollector) {
        String loc = tuple.getString(0);
        stringIntegerMap.put(loc, MapUtils.getInteger(stringIntegerMap, loc, 0) + 1);
    }

    @Override
    public void complete(Map<String, Integer> val, TridentCollector tridentCollector) {
        System.out.println("I am partition [" + partitionId + "] and have aggregated: [" + val + "]");
        tridentCollector.emit(new Values(val));
    }

    @Override
    public void prepare(Map map, TridentOperationContext tridentOperationContext) {
        this.partitionId = tridentOperationContext.getPartitionIndex();
    }

    @Override
    public void cleanup() {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
