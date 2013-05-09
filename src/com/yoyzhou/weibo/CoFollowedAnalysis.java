package com.yoyzhou.weibo;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
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
	public static class CombineCoFollowedMapper extends Mapper<Object, Text, MyWritable, LongWritable>{
		
		private final static LongWritable one = new LongWritable(1);
		private MyWritable pair = new MyWritable();
		private ArrayList<String> famousUsers = new ArrayList<String>();
		
		@Override
		public void setup(Context context){
			
			//get the followed threshold from command line, specified like -DfollowedThreshold=666
			int threshold = context.getConfiguration().getInt("followedThreshold", 1000);
			
			try{
				Path[] caches = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				for(Path cache : caches){
					
					BufferedReader reader = new BufferedReader(new FileReader(cache.toString()));
					String line = null;
					try{
						while((line = reader.readLine()) != null){
							String[] fields = line.split("\t");
							if(Integer.valueOf(fields[1]).intValue() >= threshold)
								famousUsers.add(fields[0]);
						}
					}finally{
						reader.close();
					}
				}
			}catch(IOException ioe){
					System.err.println("Exception when reading DistributedCache, " + ioe);
				}
			
		}
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			//note value is format as user:follow1,follow2,...,follown
			if (value.toString().endsWith(":")) return;
			String[] followedUsers = value.toString().split(":")[1].split(",");
			
			int followedLen = followedUsers.length;
			
			String oneUser = null;
			String anotherUser = null;
			
			
			//combine followed users into co-followed pairs
			for(int i =0; i < followedLen; i++){
				oneUser = followedUsers[i];
				if (!famousUsers.contains(oneUser))
					continue;
				for(int j = i + 1; j < followedLen; j++){
					anotherUser = followedUsers[j];
					
					//emit only when users are celebrities
					if(famousUsers.contains(anotherUser)){
						pair.set(Long.valueOf(oneUser).longValue(), Long.valueOf(anotherUser).longValue());
						context.write(pair, one);
					}
					
				}
			}
			
		}
		
	}


	public static class AggregateCoFollowedReducer extends Reducer<MyWritable, LongWritable, MyWritable, LongWritable>{
		
		private LongWritable result = new LongWritable();
		
		@Override
		public void reduce(MyWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			for (LongWritable val : values){
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
		
	}
	
	


	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 3){
			
			System.err.println(otherArgs.length);
			System.err.println("Usage: CoFollowedAnalysis <cache file> < file input> <file out>");
		    System.exit(3);
		}
		
		//conf.setInt("followedThreshold", 2000);
		DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(), conf);
		
		Job job = new Job(conf, "Weibo Co-followed Analysis");
		
		job.setJarByClass(CoFollowedAnalysis.class);
		job.setMapperClass(CombineCoFollowedMapper.class);
		job.setCombinerClass(AggregateCoFollowedReducer.class);
		job.setReducerClass(AggregateCoFollowedReducer.class);
		
		job.setMapOutputKeyClass(MyWritable.class);
	    job.setMapOutputValueClass(LongWritable.class);
	    
		job.setOutputKeyClass(MyWritable.class);
		job.setOutputValueClass(LongWritable.class);
		
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

	

}
