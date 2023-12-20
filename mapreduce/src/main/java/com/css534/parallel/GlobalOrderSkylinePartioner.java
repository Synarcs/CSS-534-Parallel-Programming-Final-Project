package com.css534.parallel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *  A Custom paritioner written for debug purpose to debug the custom paritioning implementation
 *   Used to send all the K / v pairs having same column to the same reducer.
 */
@SuppressWarnings("unused")
public class GlobalOrderSkylinePartioner extends Partitioner<GlobalOrderSkylineKey, Object> {

    private Log log = LogFactory.getLog(GlobalOrderSkylinePartioner.class);

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
