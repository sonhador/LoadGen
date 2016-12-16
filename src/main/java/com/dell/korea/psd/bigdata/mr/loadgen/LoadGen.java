/*******************************************************************************
 * The MIT License (MIT)
 *
 * Copyright (c) 2016 MongJu Jung
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *******************************************************************************/
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
        if (args.length < 4) {
            System.err.println("args: <output_hdfs_path> <bytes_per_line_to_generate> <load_gen_period_in_minutes> <number_of_mappers>");
            System.exit(1);
        }
        
        ToolRunner.run(new Configuration(), new LoadGen(), args);
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

