package com.yoyzhou.weibo;

import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.StringUtils;

/**
 * Demos per how many bytes per each built-in Writable type takes and what does
 * their bytes sequences look like
 * 
 * @author yoyzhou
 * 
 */

public class WritableBytesLengthDemo {

	public static void main(String[] args) throws IOException {

		// one billion representations by different Writable object
		IntWritable int_b = new IntWritable(1000000000);
		LongWritable long_b = new LongWritable(1000000000);
		VIntWritable vint_b = new VIntWritable(1000000000);
		VLongWritable vlong_b = new VLongWritable(1000000000);

		// serialize writable object to byte array
		byte[] bs_int_b = serialize(int_b);
		byte[] bs_long_b = serialize(long_b);
		byte[] bs_vint_b = serialize(vint_b);
		byte[] bs_vlong_b = serialize(vlong_b);

		// print byte array in hex string and their length
		String hex = StringUtils.byteToHexString(bs_int_b);
		formatPrint("IntWritable", "1,000,000,000",hex, bs_int_b.length);

		hex = StringUtils.byteToHexString(bs_long_b);
		formatPrint("LongWritable", "1,000,000,000",hex, bs_long_b.length);

		hex = StringUtils.byteToHexString(bs_vint_b);
		formatPrint("VIntWritable", "1,000,000,000",hex, bs_vint_b.length);

		hex = StringUtils.byteToHexString(bs_vlong_b);
		formatPrint("VLongWritable", "1,000,000,000", hex, bs_vlong_b.length);
		
		
		Text myText = new Text("my text");
		byte[] text_bs = serialize(myText);
		
		hex = StringUtils.byteToHexString(text_bs);
		formatPrint("Text", "\"my text\"", hex, text_bs.length);
		
		Text myText2 = new Text("我的文本");
		byte[] text2_bs = serialize(myText2);
		
		hex = StringUtils.byteToHexString(text2_bs);
		formatPrint("Text", "\"我的文本\"", hex, text2_bs.length);
		
		MyWritable customized = new MyWritable(new VLongWritable(1000), new VLongWritable(1000000000));
		byte[] customized_bs = serialize(customized);
		hex = StringUtils.byteToHexString(customized_bs);
		formatPrint("MyWritable", "new VLongWritable(1000), new VLongWritable(1000000000)", hex, customized_bs.length);
		
		
		MyWritableComparable mwc1 = new MyWritableComparable(-10033,201);
		MyWritableComparable mwc2 = new MyWritableComparable(-10033,200);
		
		MyWritableComparable.Comparator mwcc = new MyWritableComparable.Comparator();
		int cmp = mwcc.compare(serialize(mwc1), 0, 2, serialize(mwc2), 0, 2);
		
		String result = "";
		switch(cmp){
			case 0: result = "equals"; break;
			case 1: result = "greater than";break;
			case -1: result = "less than";break;
		}
		result = "MyWritableComparable(-10033,201) " + result + " MyWritableComparable(-10033,200)";
		System.out.println(result);
		
		
	}

	private static void formatPrint(String type, String param, String hex, int length) {

		String format = "%1$-50s %2$-16s with length: %3$2d%n";
		System.out.format(format, "Byte array per " + type
				+ "("+ param +") is:", hex, length);

	}

	/**
	 * Utility method to serialize Writable object, return byte array
	 * representing the Writable object
	 * 
	 * */
	public static byte[] serialize(Writable writable) throws IOException {

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(out);
		writable.write(dataOut);
		dataOut.close();

		return out.toByteArray();

	}

	/**
	 * Utility method to deserialize input byte array, return Writable object
	 * 
	 * */
	public static Writable deserialize(Writable writable, byte[] bytes)
			throws IOException {

		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		DataInputStream dataIn = new DataInputStream(in);
		writable.readFields(dataIn);

		dataIn.close();
		return writable;

	}
}
