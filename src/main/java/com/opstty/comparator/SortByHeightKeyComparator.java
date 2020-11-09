package com.opstty.comparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class SortByHeightKeyComparator implements WritableComparable<SortByHeightKeyComparator> {
    public String geopoint;
    public float height;

    public SortByHeightKeyComparator() {
    }

    public SortByHeightKeyComparator(String geopoint, float height) {
        super();
        this.set(geopoint,height);
    }

    public void set(String geopoint,float height) {
        this.geopoint = (geopoint == null) ? "" : geopoint;
        this.height = height;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(geopoint);
        out.writeFloat(height);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        geopoint = in.readUTF();
        height = in.readFloat();
    }

    @Override
    public int compareTo(SortByHeightKeyComparator o) {
        int heightComparator = Float.compare(height, o.height);
        if (heightComparator != 0) {
            return heightComparator;
        } else {
            return geopoint.toLowerCase().compareTo(o.geopoint.toLowerCase());
        }
    }

}