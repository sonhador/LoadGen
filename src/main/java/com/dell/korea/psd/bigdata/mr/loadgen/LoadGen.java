package com.dell.korea.psd.bigdata.mr.loadgen;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class LoadGen extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("args: <output_hdfs_path> <bytes_per_line_to_generate> <load_gen_period_in_minutes> <number_of_mappers>");
            System.exit(1);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration(getConf());

        conf.set("yarn.app.mapreduce.am.commands-opts", "-Xmx2048m");
        conf.set("mapreduce.map.memory.mb", "2048");
        conf.set("mapred.child.java.opts", "-Xmx2000m");

        conf.set("OUTPUT_PATH", args[0]);
        conf.setInt("LINE_BYTES", Integer.parseInt(args[1]));
        conf.setInt("LOAD_MINS", Integer.parseInt(args[2]));
        conf.setInt("MAPPERS", Integer.parseInt(args[3]));

        Job job = Job.getInstance(conf, "LoadGen");
    
        FileOutputFormat.setOutputPath(job, new Path(args[0]));

        job.setJarByClass(LoadGen.class);
        job.setMapperClass(LoadGenMapper.class);
        job.setInputFormatClass(LoadGenInputFormat.class);
        job.setNumReduceTasks(0);

        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        int completion = job.waitForCompletion(true) ? 0 : 1;

        return completion;
    }
}

