package com.edvorkin.trident

import backtype.storm.Config
import backtype.storm.LocalCluster
import backtype.storm.LocalDRPC
import backtype.storm.tuple.Fields

import storm.trident.TridentTopology

/**
 * Created with IntelliJ IDEA.
 * User: edvorkin
 * Date: 6/10/13
 * Time: 6:46 PM
 * To change this template use File | Settings | File Templates.
 */
FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout();

// A topology is a set of streams.
// A stream is a DAG of Spouts and Bolts.
// (In Storm there are Spouts (data producers) and Bolts (data processors).
// Spouts create Tuples and Bolts manipulate then and possibly emit new ones.)

// But in Trident we operate at a higher level.
// Bolts are created and connected automatically out of higher-level constructs.
// Also, Spouts are "batched".
TridentTopology topology = new TridentTopology();
// Each primitive allows us to apply either filters or functions to the stream
// We always have to select the input fields.

topology.newStream("filter", spout).each(new Fields("text", "actor"), new EugeneTweetFilter())
        .each(new Fields("text", "actor"), new Utils.PrintFilter());


Config conf = new Config();
//
		LocalDRPC drpc = new LocalDRPC();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("hackaton", conf, topology.build());