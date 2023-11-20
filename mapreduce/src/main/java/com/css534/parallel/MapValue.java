package com.css534.parallel;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MapValue implements Writable {

    private double distance;
    private int rowValue;

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
