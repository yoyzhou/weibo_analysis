package com.yoyzhou.weibo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.StringUtils;


public class WritableLengthExample {

	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		//TODO How many bytes per each built-in Writable type takes, what does their bytes sequences look like
		//IntWritable, VLongWritable, etc. to name a few and the more important is user customized Writable class
		//Two probable articles about this topic, length of /Customized/Writable class; and how to write 
		//RawComparator of user Customized Writable Class.
		
		IntWritable int8 = new IntWritable(17);
		LongWritable l = new LongWritable(17);
		CoFollowedPairWritable cfp = new CoFollowedPairWritable(1000000008, 33333);
		
		byte[] bs = serialize(cfp);
		
		cfp.set(100000000, 33333);
		byte[] bs2 = serialize(cfp);
		
		String hex = StringUtils.byteToHexString(bs2);
		System.out.println(hex);
		
		//bs = new byte[]{0x08, 0x0a};
		cfp = (CoFollowedPairWritable)deserialize(cfp, bs);
		long i = WritableUtils.readVLong(new DataInputStream(new ByteArrayInputStream(bs)));
		i = WritableComparator.readVLong(bs, 0);
		
		i = WritableUtils.decodeVIntSize(bs[0]);
		i = WritableComparator.readVLong(bs, 1);
		System.out.println(i);
		System.out.println(cfp);
		
		CoFollowedPairWritable.Comparator cc = new CoFollowedPairWritable.Comparator();
		
		i = cc.compare(bs2, 0, bs2.length, bs, 0, bs.length);
		System.out.println(i);
		
	}
	
	public static byte[] serialize(Writable writable) throws IOException{
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(out);
		writable.write(dataOut);
		dataOut.close();
		
		return out.toByteArray();
		
	}
	
	public static Writable deserialize(Writable writable, byte[] bytes) throws IOException{
		
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		DataInputStream dataIn = new DataInputStream(in);
		writable.readFields(dataIn);
		
		dataIn.close();
		return writable;
		
	}

}
