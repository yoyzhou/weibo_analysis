package com.yoyzhou.weibo;


import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class CoFollowedAnalysis {

	/**
	 * 
	 * */
	public static class CombineCoFollowedMapper extends Mapper<Object, Text, CoFollowedPairWritable, LongWritable>{
		
		private final static LongWritable one = new LongWritable(1);
		private CoFollowedPairWritable pair = new CoFollowedPairWritable();
		
		private enum Conuter {LINEREADED};
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			//note value is format as user:follow1,follow2,...,follown
			if (value.toString().endsWith(":")) return;
			String[] followedUsers = value.toString().split(":")[1].split(",");
			
			int followedLen = followedUsers.length;
			
			//combine followed users into co-followed pairs
			for(int i =0; i < followedLen; i++){
				for(int j = i + 1; j < followedLen; j++){
					pair.set(Long.valueOf(followedUsers[i]).longValue(),
							Long.valueOf(followedUsers[j]).longValue());
					context.write(pair, one);
				}
			}
			
			context.getCounter(Conuter.LINEREADED).increment(1L);
			
		}
		
	}


	public static class AggregateCoFollowedReducer extends Reducer<CoFollowedPairWritable, LongWritable, CoFollowedPairWritable, LongWritable>{
		
		private LongWritable result = new LongWritable();
		
		@Override
		public void reduce(CoFollowedPairWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			for (LongWritable val : values){
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
		
	}
	
	

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 2){
			
			System.err.println(otherArgs.length);
			System.err.println("Usage: CoFollowedAnalysis <in> <out>");
		    System.exit(2);
		}
		
		Job job = new Job(conf, "Weibo Co-followed Analysis");
		job.setJarByClass(CoFollowedAnalysis.class);
		job.setMapperClass(CombineCoFollowedMapper.class);
		job.setCombinerClass(AggregateCoFollowedReducer.class);
		job.setReducerClass(AggregateCoFollowedReducer.class);
		
		job.setMapOutputKeyClass(CoFollowedPairWritable.class);
	    job.setMapOutputValueClass(LongWritable.class);
	    
		job.setOutputKeyClass(CoFollowedPairWritable.class);
		job.setOutputValueClass(LongWritable.class);
		//job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

	

}
