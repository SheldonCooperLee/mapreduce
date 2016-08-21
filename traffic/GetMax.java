package com.liqi.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class GetMax {
	
	
	public static class GetMaxMapper
	                    extends Mapper<LongWritable, Text, Text,LongWritable>{
        private long max=Long.MIN_VALUE;
		@Override
		protected void cleanup(
				Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(new Text("max"), new LongWritable(max));
		}

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line=value.toString();
        	String[] fields=line.split("\t");
        	long tmp=Long.parseLong(fields[0]);
        	if(tmp>max){
        		max=tmp;
        	}
		}
		
	}
	
	/**
	 * @author sheldon
	 *
	 */
	public static class GetMaxReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
    
		private long max=Long.MIN_VALUE;
		@Override
		protected void cleanup(
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(new Text("max"), new LongWritable(max));
		}

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for(LongWritable value:values){
				if(value.get()>max){
					max=value.get();
				}
			}
			
		}
		
	}
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf=new Configuration();
         Job job=Job.getInstance(conf,GetMax.class.getSimpleName());
		 
		 job.setJarByClass(GetMax.class);
		 
		 FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.12.60:9000/input"));
		 job.setInputFormatClass(TextInputFormat.class);
		 job.setMapperClass(GetMaxMapper.class);
		 job.setMapOutputKeyClass(Text.class);
		 job.setMapOutputValueClass(LongWritable.class);
		 
		 job.setReducerClass(GetMaxReducer.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(LongWritable.class);
		 
		 String output="hdfs://192.168.12.60:9000/output";
		 FileOutputFormat.setOutputPath(job, new Path(output));
		 job.setOutputFormatClass(TextOutputFormat.class);
		 
		
			
	     job.waitForCompletion(true);
	}

}
