package com.css534.parallel;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class RouteParitioner extends Partitioner<MapKeys, Text> {
    @Override
    public int getPartition(MapKeys text, Text text2, int i) {
        return 0;
    }
}
