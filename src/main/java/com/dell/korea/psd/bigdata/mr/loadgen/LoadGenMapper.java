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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LoadGenMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {
	private static int FILE_IO_BUF_SIZE = 1864088;
	private byte []line;
	private byte []newline = "\n".getBytes();
	private String outputFilePath;
    private FileSystem fs;
    private FSDataOutputStream os;
    private BufferedOutputStream bos;

    private Pattern protocolPathPattern = Pattern.compile("(hdfs://[^/]+)(/.+)");
    
    @Override
    protected void setup(Mapper<LongWritable, Text, NullWritable, NullWritable>.Context ctx) throws IOException, InterruptedException {
        int lineSize = ctx.getConfiguration().getInt("LINE_BYTES", 0);

        line = new byte[lineSize];
        for (int i=0;i<lineSize; i++) {
            line[i] = "0".getBytes()[0];
        }

        outputFilePath = ctx.getConfiguration().get("OUTPUT_PATH");

        Configuration conf = new Configuration();
        Matcher matcher = protocolPathPattern.matcher(outputFilePath);
        if (matcher.find()) {
            conf.set("fs.defaultFS", matcher.group(1));
            conf.setInt("dfs.client.cached.conn.retry", 0);
            conf.setInt("dfs.replication", 1);
            conf.setInt("dfs.replication.max", 1);
            conf.setInt("dfs.replication.min", 1);
            conf.addResource(this.getClass().getResourceAsStream("hadoop/core-site.xml"), "core-site.xml");
            conf.addResource(this.getClass().getResourceAsStream("hadoop/hdfs-site.xml"), "hdfs-site.xml");

            outputFilePath = matcher.group(2);
        }

        fs = FileSystem.get(conf);

        Path file = new Path(outputFilePath+"/"+InetAddress.getLocalHost().getHostName() + "_" + Thread.currentThread().getId() + "_" + UUID.randomUUID().toString());
        os = fs.create(file, true, FILE_IO_BUF_SIZE);
        bos = new BufferedOutputStream(os, FILE_IO_BUF_SIZE);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, NullWritable>.Context ctx) throws IOException, InterruptedException {
        bos.write(line);
        bos.write(newline);

        ctx.progress();
    }
}

