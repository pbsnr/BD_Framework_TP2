package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class ExistingSpeciesMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            if (value.toString().contains("GEOPOINT") /*Some condition satisfying it is header*/)
                return;
            else {
                StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
                while (itr.hasMoreTokens()) {

                    word.set(itr.nextToken().split(";")[3]);
                    context.write(word, one);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
