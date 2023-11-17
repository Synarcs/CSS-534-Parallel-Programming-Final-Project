package com.css534.parallel;

import org.apache.hadoop.mapred.*;

import java.io.IOException;


public class GaskyJob {
    public static void main(String[] args) throws InterruptedException, IOException {

        JobConf conf = new JobConf();
        conf.setJobName("MRGasky_implementatio");
        conf.setMapperClass(GaskyMapper.class);
        conf.setReducerClass(GaskyReducer.class);
//        conf.setPartitionerClass((Class<? extends Partitioner>) RouteParitioner.class);

        JobClient client = new JobClient();

        System.out.println(conf.toString());
    }
}