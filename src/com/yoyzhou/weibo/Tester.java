package com.yoyzhou.weibo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;


public class Tester {

	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		IntWritable int8 = new IntWritable(17);
		LongWritable l = new LongWritable(17);
		
		byte[] bs = serialize(l);
		String hex = StringUtils.byteToHexString(bs);
		System.out.println(hex);
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
