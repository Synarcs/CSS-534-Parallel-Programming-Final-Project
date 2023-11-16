package com.css534.parallel;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Comparator extends WritableComparator {

    protected Comparator(Class<? extends WritableComparable> keyClass) {
        super(Text.class);
    }

    @Override
    public int compare(Object a, Object b) {
        return super.compare(a, b);
    }
}
