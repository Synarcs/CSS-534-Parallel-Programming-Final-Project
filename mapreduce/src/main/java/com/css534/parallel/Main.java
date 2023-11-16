package com.css534.parallel;

import org.apache.hadoop.mapred.*;


public class Main {
    public static void main(String[] args) {

        JobConf conf = new JobConf();
        conf.setJobName("MRGasky_implementatio");
        conf.setMapperClass(GaskyMapper.class);
        conf.setReducerClass(GaskyReducer.class);
    }
}