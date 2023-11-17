package com.css534.parallel;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@AllArgsConstructor
public class MapValue implements Writable {

    private int rowValue;
    private double distance;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(distance);
        dataOutput.writeInt(rowValue);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        rowValue = dataInput.readInt();
        distance = dataInput.readDouble();
    }
}
