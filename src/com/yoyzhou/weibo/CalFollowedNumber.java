package com.yoyzhou.weibo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;


public class CalFollowedNumber {

	public static class CalFollowedNumberMapper extends Mapper<Object, Text, Text, IntWritable>{
		
		private final static IntWritable one = new IntWritable(1);
	
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			//note value is format as user:follow1,follow2,...,follown
			if (value.toString().endsWith(":")) return;
			String[] followedUsers = value.toString().split(":")[1].split(",");

			//for each user in followedUser followed number plus 1
			for (int i = 0; i < followedUsers.length; ++i){
				context.write(new Text(followedUsers[i]), one);
				
			}
		}
			
			
		}
	public static class CalFollowedNumberReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		private IntWritable result = new IntWritable();
				
				@Override
				public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
					int sum = 0;
					for (IntWritable val : values){
						sum += val.get();
					}
					result.set(sum);
					context.write(key, result);
				}
	}
		
	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
				Configuration conf = new Configuration();
				String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
				if(otherArgs.length != 2){
					
					System.err.println(otherArgs.length);
					System.err.println("Usage: CalFollowedNumber <in> <out>");
				    System.exit(2);
				}
				
				Job job = new Job(conf, "Calculate User's Followed Number ");
				job.setJarByClass(CalFollowedNumber.class);
				job.setMapperClass(CalFollowedNumberMapper.class);
				job.setCombinerClass(CalFollowedNumberReducer.class);
				job.setReducerClass(CalFollowedNumberReducer.class);
				
				job.setMapOutputKeyClass(Text.class);
			    job.setMapOutputValueClass(IntWritable.class);
			    
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(IntWritable.class);
				//job.setNumReduceTasks(0);
				FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
				FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
				System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
