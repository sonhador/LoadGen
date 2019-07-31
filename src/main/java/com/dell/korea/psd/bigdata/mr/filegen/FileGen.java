package com.dell.korea.psd.bigdata.mr.filegen;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
  * @author MongJu Jung(mongju.jung@dell.com)
  * Created on 2019. 7. 31.
  */

public class FileGen extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("args: <output_hdfs_path> <bytes_per_file_to_generate> <number_of_files_to_generate_per_mapper> <number_of_mappers>");
            System.exit(1);
        }
        
        ToolRunner.run(new Configuration(), new FileGen(), args);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration(getConf());

        conf.set("yarn.app.mapreduce.am.commands-opts", "-Xmx2048m");
        conf.set("mapreduce.map.memory.mb", "2048");
        conf.set("mapred.child.java.opts", "-Xmx2000m");

        conf.set("OUTPUT_PATH", args[0]);
        conf.setInt("FILE_BYTES", Integer.parseInt(args[1]));
        conf.setInt("NUM_FILES", Integer.parseInt(args[2]));
        conf.setInt("MAPPERS", Integer.parseInt(args[3]));

        Job job = Job.getInstance(conf, "FileGen");
    
        FileOutputFormat.setOutputPath(job, new Path(args[0]));

        job.setJarByClass(FileGen.class);
        job.setMapperClass(FileGenMapper.class);
        job.setInputFormatClass(FileGenInputFormat.class);
        job.setNumReduceTasks(0);

        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        int completion = job.waitForCompletion(true) ? 0 : 1;

        return completion;
    }
}
