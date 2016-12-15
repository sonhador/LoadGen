package com.dell.korea.psd.bigdata.mr.loadgen;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class LoadGenInputFormat extends InputFormat<LongWritable, Text> {
    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        int mappers = job.getConfiguration().getInt("MAPPERS", 0);

        List<InputSplit> inputSplits = new ArrayList<InputSplit>();

        for (int i=0; i<mappers; i++) {
            inputSplits.add(new LoadGenInputSplit());
        }

        return inputSplits;
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext ctx) {
         return new LoadGenRecordReader();
    }
}
