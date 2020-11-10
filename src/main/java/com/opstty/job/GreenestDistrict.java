package com.opstty.job;

import com.opstty.mapper.DistrictContainingTreesMapper;
import com.opstty.mapper.GreenestDistrictMapper;
import com.opstty.mapper.GreenestDistrictMapper2;
import com.opstty.reducer.DistrictContainingTreesReducer;
import com.opstty.reducer.GreenestDistrictReducer;
import com.opstty.reducer.GreenestDistrictReducer2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class GreenestDistrict {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "districtcontainingtrees");
        job.setJarByClass(GreenestDistrict.class);
        job.setMapperClass(GreenestDistrictMapper.class);
        job.setCombinerClass(GreenestDistrictReducer.class);
        job.setReducerClass(GreenestDistrictReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));


        job.waitForCompletion(true);

        Configuration conf2 = new Configuration();

        Job job2 = Job.getInstance(conf2, "districtcontainingtrees2");
        job2.setJarByClass(GreenestDistrict.class);
        job2.setMapperClass(GreenestDistrictMapper2.class);
        job2.setCombinerClass(GreenestDistrictReducer2.class);
        job2.setReducerClass(GreenestDistrictReducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));


        System.exit(job2.waitForCompletion(true) ? 0 : 1);



    }
}
