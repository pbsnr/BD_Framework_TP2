package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WhereOldestReducer  extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int min = 2100;
        for (IntWritable val : values) {
            if (val.get() < min){
                min = val.get();
            }
        }
        result.set(min);
        context.write(key, result);
    }
}