package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class HighestTreePerSpecieMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text height = new Text();
    private String[] temp;

    public void map(Object key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
        int valeur = 0;
        System.out.println("0");
        itr.nextToken();
        while (itr.hasMoreTokens()) {
            valeur ++;
            System.out.println(valeur);
            temp = itr.nextToken().split(";");
            word.set(temp[3]);
            height.set(temp[6]);

            context.write(word, height);
        }
    }
}