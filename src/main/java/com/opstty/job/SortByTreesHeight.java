package com.opstty.job;

import com.opstty.comparator.SortByHeightComparator;
import com.opstty.comparator.SortByHeightKeyComparator;
import com.opstty.mapper.DistrictContainingTreesMapper;
import com.opstty.mapper.NbrTreesSpeciesMapper;
import com.opstty.mapper.SortByTreesHeightMapper;
import com.opstty.reducer.DistrictContainingTreesReducer;
import com.opstty.reducer.NbrTreesSpeciesReducer;
import com.opstty.reducer.SortByTreesHeightReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SortByTreesHeight {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: sort_by_trees_height <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "sort_by_trees_height");
        job.setJarByClass(SortByTreesHeight.class);

        job.setMapperClass(SortByTreesHeightMapper.class);
        job.setMapOutputKeyClass(SortByHeightKeyComparator.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setSortComparatorClass(SortByHeightComparator.class);
        job.setReducerClass(SortByTreesHeightReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
