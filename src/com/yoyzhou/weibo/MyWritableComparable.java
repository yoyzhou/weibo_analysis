package com.yoyzhou.weibo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * This MyWritable class demonstrates how to write a custom Writable class
 * */
public class MyWritableComparable implements
		WritableComparable<MyWritableComparable> {

	private VLongWritable field1;
	private VLongWritable field2;

	public MyWritableComparable() {
		this.set(new VLongWritable(), new VLongWritable());
	}

	public MyWritableComparable(long fld1, long fld2) {

		this.set(fld1, fld2);

	}

	public MyWritableComparable(VLongWritable fld1, VLongWritable fld2) {

		this.set(fld1, fld2);

	}

	public void set(VLongWritable fld1, VLongWritable fld2) {
		// make sure the smaller field is always put as field1
		if (fld1.get() <= fld2.get()) {
			this.field1 = fld1;
			this.field2 = fld2;
		} else {

			this.field1 = fld2;
			this.field2 = fld1;
		}
	}

	public void set(long fld1, long fld2) {
		// make sure the smaller field is always put as field1
		if (fld1 <= fld2) {
			this.field1 = new VLongWritable(fld1);
			this.field2 = new VLongWritable(fld2);
		} else {

			this.field1 = new VLongWritable(fld2);
			this.field2 = new VLongWritable(fld1);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {

		field1.write(out);
		field2.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		field1.readFields(in);
		field2.readFields(in);
	}

	/** Returns true if <code>o</code> is a MyWritable with the same values. */
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof MyWritableComparable))
			return false;

		MyWritableComparable other = (MyWritableComparable) o;
		return field1.equals(other.field1) && field2.equals(other.field2);

	}

	@Override
	public int hashCode() {

		return field1.hashCode() * 163 + field2.hashCode();
	}

	@Override
	public String toString() {
		return field1.toString() + "\t" + field2.toString();
	}

	/**
	 * Compare two vlong pairs, return 0 only if this field1 equals that field1
	 * and this field2 equals that field2, otherwise compare field1 then compare
	 * field2.
	 * */
	@Override
	public int compareTo(MyWritableComparable o) {
		int cmp = this.field1.compareTo(o.field1);
		if (cmp != 0) {
			return cmp;
		}
		return field2.compareTo(o.field2);
	}

	/**
	 * A RawComparator that compares serialized VlongWritable Pair
	 * compare method decode long value from serialized byte array one by one
	 * */
	public static class Comparator extends WritableComparator {

		public Comparator() {
			super(MyWritableComparable.class);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

			int cmp = 1;
			// determine how many bytes the first VLong takes
			int n1 = WritableUtils.decodeVIntSize(b1[s1]);
			int n2 = WritableUtils.decodeVIntSize(b2[s2]);

			try {
				//read value from VLongWritable byte array
				long l11 = readVLong(b1, s1);
				long l21 = readVLong(b2, s2);

				cmp = l11 > l21 ? 1 : (l11 == l21 ? 0 : -1);
				if (cmp != 0) {
					return cmp;
				} else {

					long l12 = readVLong(b1, s1 + n1);
					long l22 = readVLong(b2, s2 + n2);
					return cmp = l12 > l22 ? 1 : (l12 == l22 ? 0 : -1);
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
				
		}
	}

	static { //register this comparator
		WritableComparator.define(MyWritableComparable.class, new Comparator());
	}

}
