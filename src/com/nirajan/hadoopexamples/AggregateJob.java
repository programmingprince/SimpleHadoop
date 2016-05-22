package com.nirajan.hadoopexamples;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AggregateJob extends Configured implements Tool{

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		int rc=ToolRunner.run(new AggregateJob(), args);
		System.exit(rc);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Job job=new Job(getConf());
		job.setJarByClass(getClass());
		job.setJobName(getClass().getSimpleName());
		
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		
		job.setMapperClass(ProjectionMapper.class);
		job.setCombinerClass(LongSumReducer.class);
		job.setReducerClass(LongSumReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		return job.waitForCompletion(true)?0:1;
	}

}
