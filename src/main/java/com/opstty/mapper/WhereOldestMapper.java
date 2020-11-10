package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WhereOldestMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private String[] temp = new String[13];
    private Text word = new Text();
    private IntWritable year = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            if (value.toString().contains("GEOPOINT") /*Some condition satisfying it is header*/)
                return;
            else {
                StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
                while (itr.hasMoreTokens()) {
                    temp = itr.nextToken().split(";");
                    word.set(temp[1]);
                    year.set((int) Float.parseFloat(temp[5]));
                    context.write(word, year);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}