package com.css534.parallel;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MapKeys implements WritableComparable<MapKeys> {
    private int rowValue;
    private int colValue;
    @Override
    public int compareTo(MapKeys o) {
        int rowComparision = Integer.compare(rowValue, colValue);
        return rowComparision;
    }

    public MapKeys(int rowValue, int colValue){
        this.rowValue = rowValue;
        this.colValue = colValue;
    }

    /**
            data serialization implementation
            Mapper keys emit logic
      */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(rowValue);
        dataOutput.writeInt(colValue);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        rowValue = dataInput.readInt();
        colValue = dataInput.readInt();
    }
}
