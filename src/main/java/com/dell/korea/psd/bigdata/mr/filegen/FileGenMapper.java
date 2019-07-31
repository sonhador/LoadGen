package com.dell.korea.psd.bigdata.mr.filegen;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
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

/**
  * @author MongJu Jung(mongju.jung@dell.com)
  * Created on 2019. 7. 31.
  */

public class FileGenMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {
	private static int FILE_IO_BUF_SIZE = 1864088;
	private byte []line;
	private String outputFilePath;
    private FileSystem fs;
    private List<OutputStream> fileOutputStreams;

    private Pattern protocolPathPattern = Pattern.compile("(hdfs://[^/]+)(/.+)");
    
    @Override
    protected void setup(Mapper<LongWritable, Text, NullWritable, NullWritable>.Context ctx) throws IOException, InterruptedException {
        int lineSize = ctx.getConfiguration().getInt("FILE_BYTES", 0);

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

        fs = FileSystem.newInstance(conf);
        fileOutputStreams = new ArrayList<>();
    }
    
    private BufferedOutputStream createFile() throws IOException {
    	Path file = new Path(outputFilePath+"/"+InetAddress.getLocalHost().getHostName() + "_" + Thread.currentThread().getId() + "_" + UUID.randomUUID().toString());
    	FSDataOutputStream os = fs.create(file, true, FILE_IO_BUF_SIZE);
    	BufferedOutputStream bos = new BufferedOutputStream(os, FILE_IO_BUF_SIZE);
    	
    	return bos;
    }
    
    @Override
    protected void cleanup(Mapper<LongWritable,Text,NullWritable,NullWritable>.Context context) throws IOException ,InterruptedException {
    	try {
	    	for (OutputStream fileOutputStream : fileOutputStreams) {
	    		fileOutputStream.close();
	    	}
	    	fs.close();
    	} catch (Throwable e) {
    		// ignore
    	}
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, NullWritable>.Context ctx) throws IOException, InterruptedException {
        OutputStream out = createFile();
        fileOutputStreams.add(out);
        
    	out.write(line);
        ctx.progress();
    }
}