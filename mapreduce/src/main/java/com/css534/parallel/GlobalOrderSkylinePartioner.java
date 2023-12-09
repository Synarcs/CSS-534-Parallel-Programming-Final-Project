package com.css534.parallel;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 *  A Custom paritioner written for debug purpose to debug the custom paritioning implementation
 */
@SuppressWarnings("unused")
public class GlobalOrderSkylinePartioner extends Partitioner<GlobalOrderSkylineKey, Object> {

    @Override
    public int getPartition(GlobalOrderSkylineKey key, Object value, int numPartitions) throws IllegalArgumentException{
        // Ensure colNumber is not null
        if (key.getColNumber() == null) {
            throw new IllegalArgumentException("colNumber cannot be null");
        }

        // Use colNumber to determine the partition
        return Math.abs(key.getColNumber().hashCode() % numPartitions);
    }
}
