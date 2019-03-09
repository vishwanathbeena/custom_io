package com.practice.customio;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WholeFileRecordReader extends RecordReader<Text, Text> {

	private FileSplit filesplit;
	private Configuration conf;
	private Text value = new Text();
	private boolean processed = false;
	private Text key = new Text();
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Text getCurrentKey() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return processed ? 1.0f : 0.0f ;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		this.filesplit=(FileSplit) split;
		this.conf=context.getConfiguration();
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	    if (!processed) {
	        byte[] contents = new byte[(int) filesplit.getLength()];
	        Path file = filesplit.getPath();
	        String fileName=filesplit.getPath().getName();
	        FileSystem fs = file.getFileSystem(conf);
	        FSDataInputStream in = null;
	        try {
	          in = fs.open(file);
	          IOUtils.readFully(in, contents, 0, contents.length);
	          value.set(contents, 0, contents.length);
	          key.set(fileName);
	        } finally {
	          IOUtils.closeStream(in);
	        }
	        processed = true;
	        return true;
	      }
		return false;
	}

}
