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
        mins = ctx.getConfiguration().getInt("LOAD_MINS", 0);
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

