package com.yoyzhou.weibo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VectorWritable;


/**
 * This class uses k-means algorithm of Mahout to clustering weibo account based on
 * co-occurrence theory, in this case user co-followed counts is using as co-occurrence
 * measurements.
 *
 * @author yoyzhou
 * @date May 21, 2013
 *
 */

public class CoFollowedClustering {
	
	//features maintains the dimensions, key is the accountId, value is its dimension
	//note no multi-threading involved here just use HashMap
	private final static HashMap<String, Integer> features = new HashMap<String, Integer>();
	
	private static int demensions = 0; //this will +1 when encountering a new account
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		
		int k = 5; //how many clusters you want to cluster into
		
		ArrayList<NamedVector> vectors = buildCoFollowedVectors("dataset/part-r-00000");
		
		File data = new File("data/vectors");
		if(!data.exists()) data.mkdirs();
	
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		writeVectorsToSeqFile(vectors, "data/vectors/points", fs, conf);
		
		//write initial clusters point, in this case 5 points/vectors
		Path path = new Path("data/clusters/part-00000");
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path,
				Text.class, Kluster.class);

		for (int i = 0; i < k; i++) {
			NamedVector nVec =  vectors.get(i);
			
			Kluster cluster = new Kluster(nVec, i, new CosineDistanceMeasure());
			writer.append(new Text(cluster.getIdentifier()), cluster);
		}
		writer.close();
		
		//here runs the k-means clustering
		KMeansDriver.run(conf, new Path("data/vectors"), new Path(
				"data/clusters"), new Path("output"),
				new CosineDistanceMeasure(), 0.001, 10, true, 0, false);

		//read out the results from sequence file
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(
				"output/" + Kluster.CLUSTERED_POINTS_DIR + "/part-m-00000"),
				conf);
		
		IntWritable clusterId = new IntWritable();
		WeightedVectorWritable value = new WeightedVectorWritable();
		
		while (reader.next(clusterId, value)) {
			NamedVector nVec = (NamedVector)value.getVector();
			System.out.println(nVec.getName() + " belongs to cluster "
					+ clusterId.toString());
		}
		reader.close();
		
		
	}
	
	/**
	 * Write Vectors to Hadoop sequence file, it's required per Mahout implementation that Vectors passed
	 * into Mahout clusterings must be in Hadoop sequence file. 
	 * 
	 * */
	private static void writeVectorsToSeqFile(ArrayList<NamedVector> vectors, String filename,
			FileSystem fs, Configuration conf) throws IOException {
		
		Path path = new Path(filename);
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path,
				Text.class, VectorWritable.class);

		
		VectorWritable vec = new VectorWritable();
		
		//write vectors to file, the key is the account id, value is the co-followed vector of the key
		for (NamedVector entry : vectors) {
			vec.set(entry);
			writer.append(new Text(entry.getName()), vec);
		}
		writer.close();
		
	}
	
	/**
	 * Build co-followed vectors of weibo users
	 * 
	 * @return NamedVector list contains all the co-followed vectors
	 * */
	private static ArrayList<NamedVector> buildCoFollowedVectors(String cofollowedfile){
		
		ArrayList<NamedVector> coFollowedVectors = 	new ArrayList<NamedVector>();

		
		HashMap<String, ArrayList<CoFollowedNode>> coFollowedMap = buildCoFollowedMap(cofollowedfile);
		Iterator<Map.Entry<String,ArrayList<CoFollowedNode>>> itr = 
				coFollowedMap.entrySet().iterator();
		while(itr.hasNext()){
			Map.Entry<String,ArrayList<CoFollowedNode>> entry = itr.next();
			String account = entry.getKey();
			ArrayList<CoFollowedNode> cflist = entry.getValue();
			
			//NamedVector is a Vector Wrapper class which owns a name for each vector, very convenient class 
			NamedVector vec = new NamedVector(new RandomAccessSparseVector(cflist.size() + 1), account);
			Iterator<CoFollowedNode> nodeItr = cflist.iterator();
			while(nodeItr.hasNext()){
				CoFollowedNode cfnode= nodeItr.next();
				//set vector's n-dimension with co-followed number
				vec.set(cfnode.getDemension(), cfnode.getCoFollowedNum());
			}
			
			coFollowedVectors.add(vec);
			
		}
		return coFollowedVectors;
		
	}
	
	/**
	 * Parse the co-followed file, returns a HashMap whose key is the user accountId and value is a list of 
	 * CoFollowedNode, in which each CoFollowedNode represents a user with who the current user(the key)
	 * co-followed.
	 * 
	 * Note the input co-followed file format must follows format "acountId1\taccountId2\tco_followed_counts",
	 * such as: "12345	67890	33", where 33 is the number of times 12345 and 67890 co-followed.
	 * 
	 * */
	private static HashMap<String, ArrayList<CoFollowedNode>> buildCoFollowedMap(String cofollowedfile){
		
		HashMap<String, ArrayList<CoFollowedNode>> coFollowedMap = 
				new HashMap<String, ArrayList<CoFollowedNode>>();
		String line = null;
		BufferedReader br = null;
		
		try {
			
			br = new BufferedReader(new FileReader(cofollowedfile));
			
			while((line = br.readLine()) != null){
				String[] fields = line.split("\t");
				String account = fields[0];
				String anotherAccount = fields[1];
				int coFollowedNum = Integer.valueOf(fields[2]).intValue();
				
				//save dimensions
				if(!features.containsKey(account)) 
					features.put(account, demensions++);
				if(!features.containsKey(anotherAccount)) 
					features.put(anotherAccount, demensions++);
				
				CoFollowedNode cfnode = new CoFollowedNode(anotherAccount, coFollowedNum, features.get(anotherAccount));
				if (coFollowedMap.containsKey(account)){
					coFollowedMap.get(account).add(cfnode);
				}else{
					ArrayList<CoFollowedNode> cflist = new ArrayList<CoFollowedNode>();
					cflist.add(cfnode);
					coFollowedMap.put(account, cflist);
				}
				
				//add another side of co-followed map. it's a 2-ways co-followed 
				cfnode = new CoFollowedNode(account, coFollowedNum, features.get(account));
				if (coFollowedMap.containsKey(anotherAccount)){
					coFollowedMap.get(anotherAccount).add(cfnode);
				}else{
					ArrayList<CoFollowedNode> cflist = new ArrayList<CoFollowedNode>();
					cflist.add(cfnode);
					coFollowedMap.put(anotherAccount, cflist);
				}
				
			}
			
			br.close();
			return coFollowedMap;
			
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Exceptions when reading co-followed file or the  file is not found.");
		}finally{
			try {
				br.close();
			} catch (IOException e) {	}
		}
	}
	/**
	 * Inner class which represents a/one co-followed node
	 * 
	 * */
	static final class CoFollowedNode{
			
		//another weibo account of co-followed pair
		private  String accountID;  
		//number of times they are co-followed
		private int coFollowedNum; 
		//dimension in the mahout vector, this field is critical per setting up mahout vector
		private int demension;	 
		
		public CoFollowedNode(){}
		
		public CoFollowedNode(String coFollowedAccountID, int coFollowedNum, int demension){
			set(coFollowedAccountID, coFollowedNum, demension);
		}
		
		public void set(String coFollowedAccountID, int coFollowedNum, int demension){
			this.accountID = coFollowedAccountID;
			this.coFollowedNum = coFollowedNum;
			this.demension = demension;
		}
		
		//getter
		public String getcoFollowedAccountID(){ return this.accountID;}
		public int getCoFollowedNum(){return this.coFollowedNum;}
		public int getDemension(){return this.demension;}
		
	}
	
	
}
