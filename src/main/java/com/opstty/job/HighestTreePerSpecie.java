package com.opstty.job;

import com.opstty.mapper.ExistingSpeciesMapper;
import com.opstty.mapper.HighestTreePerSpecieMapper;
import com.opstty.reducer.ExistingSpeciesReducer;
import com.opstty.reducer.HighestTreePerSpecieReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HighestTreePerSpecie {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: highest_tree_per_specie <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "highest_tree_per_specie");
        job.setJarByClass(HighestTreePerSpecie.class);
        job.setMapperClass(HighestTreePerSpecieMapper.class);
        job.setCombinerClass(HighestTreePerSpecieReducer.class);
        job.setReducerClass(HighestTreePerSpecieReducer.class);
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
