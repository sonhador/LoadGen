package com.dell.korea.psd.bigdata.mr.loadgen;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class LoadGenInputSplit extends InputSplit implements Writable {
    @Override
    public long getLength() {
         return 0;
    }

    @Override
    public String[] getLocations() {
        return new String[0];
    }

	public void readFields(DataInput arg0) throws IOException {
	}

	public void write(DataOutput arg0) throws IOException {
	}
}