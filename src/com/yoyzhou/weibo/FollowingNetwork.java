package com.yoyzhou.weibo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



/**
	Created on April 23, 2013

	@author: yoyzhou
*/

/**
 * Mapper only class to extract following network from following_ntk file.
 * Line format is following_ntk.txt is:  uid : following_uid1, following_uid2,...following_uidn, means 
 * uid follows following_uid1 to followinf_uidn
 * 
 * */
public class FollowingNetwork {

public static class FollowingNetworkMapper extends Mapper<Object, Text, Text, NullWritable>{
		
			
	private ArrayList<String> famousUsers = new ArrayList<String>();
	private enum USER{NUMBER};
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
			
			String user = value.toString().split(":")[0];	//the user
			//we only care about famous users, just pass away diaosi
			if (!famousUsers.contains(user)) return;
			
			String[] followedUsers = value.toString().split(":")[1].split(",");		//the following list
			
			//for each user in followedUser followed number plus 1
			for (int i = 0; i < followedUsers.length; ++i){
				//following network is directed network, this tricky is for gephi analysis sake
				context.write(new Text(user + "\t" + followedUsers[i] + "\tDirected"), NullWritable.get());
				
			}
			
			context.getCounter(USER.NUMBER).increment(1);
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
				if(otherArgs.length != 3){
					
					System.err.println(otherArgs.length);
					System.err.println("Usage: FollowingNetwork <cache file>  <in> <out>");
				    System.exit(3);
				}
				
				//conf.setInt("followedThreshold", 2000);
				DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(), conf);
				
				Job job = new Job(conf, "Extract User‘s Following Network");
				job.setJarByClass(FollowingNetwork.class);
				job.setMapperClass(FollowingNetworkMapper.class);
				job.setMapOutputKeyClass(Text.class);
			    job.setMapOutputValueClass(NullWritable.class);
			    
			    job.setNumReduceTasks(0);
				
				//job.setNumReduceTasks(0);
				FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
				FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
				System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
