package com.nirajan.hadoopexamples;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProjectionMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
	private Text word=new Text();
	private LongWritable count=new LongWritable();
	
	@Override
	protected void map(LongWritable key,Text values,Context context) throws IOException,InterruptedException{
		String[] words=values.toString().split("\t");
		word.set(words[0]);
		count.set(Long.parseLong(words[2]));
		context.write(word, count);
	}
}
