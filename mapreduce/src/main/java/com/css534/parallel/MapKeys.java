package com.css534.parallel;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class MapKeys implements WritableComparable<MapKeys> {

    private String featureName;
    private int colValue;
    // private int rowValue;


    public int getColValue() {
        return colValue;
    }

    public String getFeatureName() {
        return featureName;
    }

    public MapKeys(){}

    @Override
    public int compareTo(MapKeys other) {
        // we need to compare the row value since the column will decide which parition to select
        // now each of the value in it has to be sorted based the row index
        // we dont need a custom sorter or comparator
        int cmp = featureName.compareTo(other.featureName);
        if (cmp == 0) {
            cmp = Integer.compare(colValue, other.colValue);
        }
        return cmp;
    }

    public MapKeys(String featureName ,int colValue){
        this.colValue = colValue;
        this.featureName = featureName;
    }

    /**
     data serialization implementation
     Mapper keys emit logic
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(featureName);
        // dataOutput.writeInt(rowValue);
        dataOutput.writeInt(colValue);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        featureName = dataInput.readUTF();
        // rowValue = dataInput.readInt();
        colValue = dataInput.readInt();
    }
}
