package com.css534.parallel;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class MapValue implements Writable, Serializable {

    private double distance;
    private int rowValue;
    public MapValue(){}
    public MapValue(double distance, int rowValue){ this.distance = distance; this.rowValue = rowValue;}

    public double getDistance() {
        return distance;
    }

    public int getRowValue() {
        return rowValue;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(distance);
        dataOutput.writeInt(rowValue);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        distance = dataInput.readDouble();
        rowValue = dataInput.readInt();
    }
}
