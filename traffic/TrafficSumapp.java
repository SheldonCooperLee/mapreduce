package com.liqi.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TrafficSumapp {
	//<k1> <k2>
	//<k3><v3> k3是手机号  v3 是流量汇总
	//<k2><v2> k2是手机号 v2 单次通讯的流量
	
	
	
	/**
	 * 内部构造一个用来计算装载流量一个类
	 * @author sheldon
	 *
	 */
	public static class TrafficWriteable implements Writable{

		public long upPackNum;  //上行数据包
		public long downPackNum; //下行数据包
		public long upPayLoad; //上行流量
		public long downPayLoad; //下行流量
		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeLong(upPackNum);
			out.writeLong(downPackNum);
			out.writeLong(upPayLoad);
			out.writeLong(downPayLoad);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			this.upPackNum=in.readLong();
			this.downPackNum=in.readLong();
			this.upPayLoad=in.readLong();
			this.downPayLoad=in.readLong();
		}

		@Override
		public String toString() {
			return this.upPackNum+"\t"+this.downPackNum+"\t"
		                       +this.upPayLoad+"\t"+this.downPayLoad;
		}
		
		public void set(long upPackNum,long downPackNum,long upPayLoad,Long downPayLoad){
			this.upPackNum=upPackNum;
			this.downPackNum=downPackNum;
			this.upPayLoad=upPayLoad;
			this.downPayLoad=downPayLoad;
		}
		public void set(String upPackNum,String downPackNum,String upPayLoad,String downPayLoad){
			this.upPackNum=Long.parseLong(upPackNum);
			this.downPackNum=Long.parseLong(downPackNum);
			this.upPayLoad=Long.parseLong(upPayLoad);
			this.downPayLoad=Long.parseLong(downPayLoad);
		}
 	}
	
	
	
	public static class TrafficMapper
	       extends Mapper<LongWritable, Text, Text, TrafficWriteable>{
        Text phoneNum=new Text();
        TrafficWriteable traffic=new TrafficWriteable();
		@Override
		protected void map(
				LongWritable key,
				Text value,
				Mapper<LongWritable, Text, Text, TrafficWriteable>.Context context)
				throws IOException, InterruptedException {
		   String lines=value.toString();
		   String[] fields=lines.split("\t");
		   int traffictype=fields.length;
		   
		   phoneNum.set(fields[1]);
		   if(traffictype==11){
			   traffic.set(fields[6], fields[7], fields[8], fields[9]);
		   }else if(traffictype==10){
			   traffic.set(fields[5], fields[6], fields[7], fields[8]);
		   }else{
			   traffic.set(fields[4], fields[5], fields[6], fields[7]);
		   }
		   context.write(phoneNum, traffic);
		}
		
	}
	
	 public static class TrafficReducer extends Reducer<Text, TrafficWriteable,Text, TrafficWriteable>{
   
		 TrafficWriteable trafficsum=new TrafficWriteable();
		@Override
		protected void reduce(
				Text key,
				Iterable<TrafficWriteable> values,
				Reducer<Text, TrafficWriteable, Text, TrafficWriteable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			long tmpupPackNum=0L;  //上行数据包
		    long tmpdownPackNum=0L; //下行数据包
		    long tmpupPayLoad=0L; //上行流量
		    long tmpdownPayLoad=0L; //下行流量
		    for(TrafficWriteable traffictmp:values){
		    	tmpupPackNum+=traffictmp.upPackNum;
		    	tmpdownPackNum+=traffictmp.downPackNum;
		    	tmpupPayLoad+=traffictmp.upPayLoad;
		    	tmpdownPayLoad+=traffictmp.downPayLoad;
		    }
		    
		    trafficsum.set(tmpupPackNum, tmpdownPackNum, tmpupPayLoad, tmpdownPayLoad);
		    context.write(key, trafficsum);
		}
		 
		 
	 } 
	 
	 public static void main(String args[]) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException{
		 Configuration conf=new Configuration();
		 Job job=Job.getInstance(conf,TrafficSumapp.class.getSimpleName());
		 
		 job.setJarByClass(TrafficSumapp.class);
		 
		 FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.12.60:9000/traffic"));
		 job.setInputFormatClass(TextInputFormat.class);
		 job.setMapperClass(TrafficMapper.class);
		 job.setMapOutputKeyClass(Text.class);
		 job.setMapOutputValueClass(TrafficWriteable.class);
		 
		 job.setReducerClass(TrafficReducer.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(TrafficWriteable.class);
		 
		 String output="hdfs://192.168.12.60:9000/output";
		 FileOutputFormat.setOutputPath(job, new Path(output));
		 job.setOutputFormatClass(TextOutputFormat.class);
		 
		 deleteOutDir(conf, output);
			
	     job.waitForCompletion(true);
		 
	 }
	
	 public static void deleteOutDir(Configuration conf, String OUT_DIR)
				throws IOException, URISyntaxException {
			FileSystem fileSystem = FileSystem.get(new URI(OUT_DIR), conf);
			if(fileSystem.exists(new Path(OUT_DIR))){
				fileSystem.delete(new Path(OUT_DIR), true);
			}
		}
}
