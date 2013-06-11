package com.edvorkin.trident

import backtype.storm.Config
import backtype.storm.LocalCluster
import backtype.storm.LocalDRPC
import backtype.storm.drpc.LinearDRPCTopologyBuilder
import backtype.storm.tuple.Fields
import backtype.storm.utils.DRPCClient
import storm.trident.TridentState
import storm.trident.TridentTopology
import storm.trident.operation.builtin.Count
import storm.trident.testing.MemoryMapState
import storm.trident.testing.Split

/**
 * Created with IntelliJ IDEA.
 * User: edvorkin
 * Date: 6/11/13
 * Time: 10:57 AM
 * To change this template use File | Settings | File Templates.
 */
FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout();
TridentTopology topology = new TridentTopology();
LocalDRPC drpc = new LocalDRPC();

    topology.newDRPCStream("words",drpc)
            .each(new Fields("args"), new Split(), new Fields("word"))
            .groupBy(new Fields("word"))
            .aggregate(new Fields("word"), new Count(), new Fields("count"))




Config conf = new Config();
//
LocalCluster cluster = new LocalCluster();
cluster.submitTopology("drpc-demo", conf, topology.build());

String text=this.class.classLoader.getResourceAsStream('500_sentences_en.txt').text


System.out.println("Results for 'hello':" + drpc.execute("words", text));

cluster.shutdown();
drpc.shutdown();

