package com.yoyzhou.weibo;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.math.NamedVector;

/**
 *
 * @author yoyzhou
 * @date May 27, 2013
 *
 */

public class ClusteringResultsReader {

	/**
	 * read out the results from sequence file
	 * @throws IOException 
	 * */
	public static void read(String resultsPath, String outputPath) throws IOException {

		BufferedWriter bw = new BufferedWriter(new FileWriter(outputPath));
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(resultsPath), conf);
		
		IntWritable clusterId = new IntWritable();
		WeightedVectorWritable value = new WeightedVectorWritable();

		while (reader.next(clusterId, value)) {
			NamedVector nVec = (NamedVector) value.getVector();
			bw.write(nVec.getName() + "\t" + clusterId );
			bw.newLine();
			System.out.println(nVec.getName() + " belongs to cluster "
					+ clusterId.toString());
		}
		reader.close();
		bw.flush();
		bw.close();
	}
			
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		ClusteringResultsReader.read("m_output/output/clusteredPoints/part-m-00000", "results.txt");
	}

}
