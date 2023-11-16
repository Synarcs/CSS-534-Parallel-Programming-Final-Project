package com.css534.parallel;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class RouteParitioner extends Partitioner<Text, Text> {
    @Override
    public int getPartition(Text text, Text text2, int i) {
        return 0;
    }
}
