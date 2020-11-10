package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GreenestDistrictReducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();
    private int max = 0;
    private int maxdistrict;
    private int value = 0;
    private int district = 0;
    private int temp = 0;
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {




        for (IntWritable val : values) {

            temp = val.get();
            if(temp > 100){
                value = temp%100;
                district = (temp - value)/100;
            }else{
                maxdistrict = temp;
            }


            if (value > max){
                max = value;
                maxdistrict = district;
            }


        }
        result.set(maxdistrict);
        context.write(key, result);


    }
}
