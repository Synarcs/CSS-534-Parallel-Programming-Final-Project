package com.css534.parallel;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *  A custom composite key with custom compartor use ful for internal hadoop comparator to order values in keys
 */
public class GlobalOrderSkylineKey implements WritableComparable<GlobalOrderSkylineKey> {
    private Integer rowNumber;
    private Integer colNumber;;


    public GlobalOrderSkylineKey(){}
    public GlobalOrderSkylineKey(Integer rowNumber, Integer colNumber){
        this.rowNumber = rowNumber;
        this.colNumber = colNumber;
    }

    @Override
    /**
     *  Use this comparator to compare the row value first and then compare the column for this key.
     */
    public int compareTo(GlobalOrderSkylineKey o) {
        int cmp = rowNumber.compareTo(o.rowNumber);
        if (cmp == 0) {
            cmp = Integer.compare(colNumber, o.colNumber);
        }
        return cmp;
    }

    public Integer getColNumber() {
        return colNumber;
    }

    public Integer getRowNumber() {
        return rowNumber;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(rowNumber);
        dataOutput.writeInt(colNumber);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        rowNumber = dataInput.readInt();
        colNumber = dataInput.readInt();
    }
}
