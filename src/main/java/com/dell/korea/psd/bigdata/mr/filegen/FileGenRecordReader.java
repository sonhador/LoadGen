package com.dell.korea.psd.bigdata.mr.filegen;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
  * @author MongJu Jung(mongju.jung@dell.com)
  * Created on 2019. 7. 31.
  */

public class FileGenRecordReader extends RecordReader<LongWritable, Text> {
	private int fileSize;
    private int numFiles;
    private int filesCreated;
    private long posBytes = 0;
    private LongWritable pos = new LongWritable();
    private Text val = new Text();

    @Override
     public void initialize(InputSplit split, TaskAttemptContext ctx) throws IOException, InterruptedException {
        numFiles = ctx.getConfiguration().getInt("NUM_FILES", 0);
        fileSize = ctx.getConfiguration().getInt("FILE_BYTES", 0);
        filesCreated = 0;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (numFiles == filesCreated) {
            return false;
        }

        filesCreated++;
        
        return true;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        posBytes += fileSize;
        pos.set(posBytes);

        return pos;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return val;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return (float)filesCreated / numFiles;
    }

    @Override
    public void close() throws IOException {

    }
}