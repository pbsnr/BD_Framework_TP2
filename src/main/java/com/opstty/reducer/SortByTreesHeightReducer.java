package com.opstty.reducer;

import com.opstty.comparator.SortByHeightKeyComparator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortByTreesHeightReducer extends Reducer<SortByHeightKeyComparator, NullWritable, Text, NullWritable> {

    private Text result;

    public void reduce(SortByHeightKeyComparator keyH, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        result = new Text (String.format("%s %.3f", keyH.geopoint, keyH.height));
        context.write(result,NullWritable.get());
    }
}