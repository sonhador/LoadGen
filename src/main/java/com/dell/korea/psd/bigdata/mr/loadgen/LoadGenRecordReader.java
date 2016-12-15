package com.dell.korea.psd.bigdata.mr.loadgen;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class LoadGenRecordReader extends RecordReader<LongWritable, Text> {
	private int mins;
	private int lineSize;
    private long startTime;
    private long posBytes = 0;
    private LongWritable pos = new LongWritable();
    private Text val = new Text();

    @Override
     public void initialize(InputSplit split, TaskAttemptContext ctx) throws IOException, InterruptedException {
        mins = ctx.getConfiguration().getInt("LOAD_MIN", 0);
        lineSize = ctx.getConfiguration().getInt("LINE_BYTES", 0);
        startTime = System.currentTimeMillis();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if ((System.currentTimeMillis() - startTime) > mins * 60 * 1000) {
            return false;
        }

        return true;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        posBytes += lineSize;
        pos.set(posBytes);

        return pos;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return val;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return ((float)(System.currentTimeMillis() - startTime)) / (mins * 60 * 1000);
    }

    @Override
    public void close() throws IOException {

    }
}

