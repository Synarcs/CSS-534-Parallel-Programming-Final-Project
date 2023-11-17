package com.css534.parallel;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


/*
        Custom Parition Key and the value is the Distance Key
 */
public class RouteComparator extends WritableComparator {

    protected RouteComparator(){
        super(MapKeys.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        MapKeys key1 = (MapKeys) a;
        MapKeys key2 = (MapKeys) b;

        int cmp = key1.getFeatureName().compareTo(key2.getFeatureName());
        if (cmp == 0) {
            cmp = Integer.compare(key1.getColValue(), key2.getColValue());
            if (cmp == 0) {
                cmp = Integer.compare(key1.getRowValue(), key2.getRowValue());
            }
        }
        return cmp;
    }
}
