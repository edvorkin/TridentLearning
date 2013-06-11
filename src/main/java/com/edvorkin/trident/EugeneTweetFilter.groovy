package com.edvorkin.trident

import storm.trident.operation.Filter
import storm.trident.operation.TridentOperationContext
import storm.trident.tuple.TridentTuple

/**
 * Created with IntelliJ IDEA.
 * User: edvorkin
 * Date: 6/10/13
 * Time: 6:22 PM
 * To change this template use File | Settings | File Templates.
 */
class EugeneTweetFilter implements Filter {
    int partitionIndex;
    @Override
    boolean isKeep(TridentTuple tuple) {
        boolean filter = tuple.getString(1).equals("eugene");
        if(filter) {
            System.err.println("I am partition [" + partitionIndex + "] and I have filtered eugene.");
        }
        return filter;
    }

    @Override
    void prepare(Map map, TridentOperationContext tridentOperationContext) {
        partitionIndex= tridentOperationContext.getPartitionIndex()
    }

    @Override
    void cleanup() {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
