package com.opstty.mapper;


import com.opstty.comparator.SortByHeightKeyComparator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;



public class SortByTreesHeightMapper extends Mapper<Object, Text, SortByHeightKeyComparator, NullWritable>  {

    private String row = "";
    private String geopoint;
    private float height;

    private SortByHeightKeyComparator heightkey = new SortByHeightKeyComparator();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            if (value.toString().contains("GEOPOINT") /*Some condition satisfying it is header*/)
                return;
            else {
                StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
                while (itr.hasMoreTokens()) {
                    row = itr.nextToken();
                    geopoint = row.split(";")[0];
                    if (!row.split(";")[6].isEmpty()){
                        height = Float.parseFloat(row.split(";")[6]);
                    }
                    heightkey.set(geopoint,height);
                    context.write(heightkey,NullWritable.get());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}