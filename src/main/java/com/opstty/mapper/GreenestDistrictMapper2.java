package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class GreenestDistrictMapper2 extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
//    private Text word = new Text();
//    private Text word_bis = new Text();

    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        try {
                StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
                while (itr.hasMoreTokens()) {

//                    word.set(itr.nextToken().split("\t")[0]);
                    String token = itr.nextToken();
                    one.set(Integer.parseInt(token.split("\t")[0])*100 + Integer.parseInt(token.split("\t")[1]));
                    context.write(new Text("The greenest district is : "), one);
                }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
