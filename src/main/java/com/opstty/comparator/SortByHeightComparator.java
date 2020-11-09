package com.opstty.comparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortByHeightComparator extends WritableComparator {

    public SortByHeightComparator() {
        super(SortByHeightKeyComparator.class, true);
    }

    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {

        SortByHeightKeyComparator key1 = (SortByHeightKeyComparator) wc1;
        SortByHeightKeyComparator key2 = (SortByHeightKeyComparator) wc2;

        int heightComparator = Float.compare(key1.height, key2.height);
        if (heightComparator != 0) {
            return heightComparator;
        } else {
            return key1.geopoint.toLowerCase().compareTo(key2.geopoint.toLowerCase());
        }
    }
}