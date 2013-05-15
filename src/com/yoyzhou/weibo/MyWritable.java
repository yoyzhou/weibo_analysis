package com.yoyzhou.weibo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;

/**
 *This MyWritable class demonstrates how to write a custom Writable class 
 * */
public class MyWritable implements Writable{
		
		
		private VLongWritable field1;
		private VLongWritable field2;
		
		public MyWritable(){
			this.set(new VLongWritable(), new VLongWritable());
		}
		
		public MyWritable(VLongWritable fld1, VLongWritable fld2){
			
			this.set(fld1, fld2);
			
		}
		
		public void set(VLongWritable fld1, VLongWritable fld2){
			//make sure the smaller field is always put as field1
			if(fld1.get() <= fld2.get()){
				this.field1 = fld1;
				this.field2 = fld2;
			}else{
				
				this.field1 = fld2;
				this.field2 = fld1;
			}
		}
		
		
		//How to write and read MyWritable fields from DataOutput and DataInput stream
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
		    if (!(o instanceof MyWritable))
		    	return false;
		    
		    MyWritable other = (MyWritable)o;
		    return field1.equals(other.field1) && field2.equals(other.field2);
		    
		  }
		
		@Override
		public int hashCode(){
			
			return field1.hashCode() * 163 + field2.hashCode();
		}
		
		@Override
		public String toString() {
			return field1.toString() + "\t" + field2.toString();
		}
		
		

	}

