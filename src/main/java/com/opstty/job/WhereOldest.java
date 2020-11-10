package com.opstty.job;

import com.opstty.comparator.SortByHeightComparator;
import com.opstty.comparator.SortByHeightKeyComparator;
import com.opstty.mapper.NbrTreesSpeciesMapper;
import com.opstty.mapper.SortByTreesHeightMapper;
import com.opstty.mapper.WhereOldestMapper;
import com.opstty.reducer.NbrTreesSpeciesReducer;
import com.opstty.reducer.SortByTreesHeightReducer;
import com.opstty.reducer.WhereOldestReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WhereOldest {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: where_oldest <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "where_oldest");
        job.setJarByClass(WhereOldest.class);
        job.setMapperClass(WhereOldestMapper.class);
        job.setCombinerClass(WhereOldestReducer.class);
        job.setReducerClass(WhereOldestReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
