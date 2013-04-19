package com.yoyzhou.weibo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;


/**
 * Revision:
 * MM/DD/YY		Descriptions
 * 04/19/13		Change data type of user1 and user2 to VLongWritable to take less memory/disk spaces.
 * 
 * */
public class CoFollowedPairWritable implements WritableComparable<CoFollowedPairWritable>{
		
		
		private VLongWritable user1;
		private VLongWritable user2;
		
		public CoFollowedPairWritable(){
			this.set(new VLongWritable(), new VLongWritable());
		}
		
		public CoFollowedPairWritable(long usr1, long usr2){
			
			this.set(usr1, usr2);
			
		}
		
		public CoFollowedPairWritable(VLongWritable usr1, VLongWritable usr2){
			
			this.set(usr1, usr2);
			
		}
		
		public void set(VLongWritable usr1, VLongWritable usr2){
			//make sure the smaller user(uid is smaller) is user1
			if(usr1.get() <= usr2.get()){
				this.user1 = usr1;
				this.user2 = usr2;
			}else{
				
				this.user1 = usr2;
				this.user2 = usr1;
			}
		}
		
		public void set(long usr1, long usr2){
			//make sure the smaller user(uid is smaller) is user1
			if(usr1 <= usr2){
				this.user1 = new VLongWritable(usr1);
				this.user2 = new VLongWritable(usr2);
			}else{
				
				this.user1 = new VLongWritable(usr2);
				this.user2 = new VLongWritable(usr1);
			}
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			
			user1.write(out);
			user2.write(out);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			
			user1.readFields(in);
			user2.readFields(in);
		}

		/** Returns true if <code>o</code> is a CoFollowedPair with the same value. */
		@Override
		  public boolean equals(Object o) {
		    if (!(o instanceof CoFollowedPairWritable))
		    	return false;
		    
		    CoFollowedPairWritable other = (CoFollowedPairWritable)o;
		    return user1.equals(other.user1) && user2.equals(other.user2);
		    
		  }
		
		@Override
		public int hashCode(){
			
			return user1.hashCode() * 163 + user2.hashCode();
		}
		
		@Override
		public String toString() {
			return user1.toString() + "\t" + user2.toString();
		}
		
		/**
		 * Compare two co-followed pairs, return 0 only if this user1 equals that user1 and this user2 equals that user2,
		 * otherwise compare user1 then compare user2. 
		 * */
		@Override
		public int compareTo(CoFollowedPairWritable o) {
			int cmp = this.user1.compareTo(o.user1);
			if(cmp != 0){
				return cmp;
			}
			return user2.compareTo(o.user2);
		}
		
		//TODO implements Comparator class for raw byte compare
		
		/** A Comparator that compares serialized IntPair. */ 
	    public static class Comparator extends WritableComparator {
	    	
	    	public Comparator() {
	    		super(CoFollowedPairWritable.class);
	    	}
	    
	    public int compare(	byte[] b1, int s1, int l1,
	                         			byte[] b2, int s2, int l2) {
	    	 
	    		 //determine how many bytes the first VLong takes
	    		 int n1 = WritableUtils.decodeVIntSize(b1[s1]);
	    		 int n2 = WritableUtils.decodeVIntSize(b2[s2]);
	    		 
	    		 int cmp = compareBytes(b1, s1, n1, b2, s2, n2);
	    		 if(cmp != 0){
	    			 return cmp;
	    		 }
	    		 return compareBytes(b1, s1+n1, l1-n1, b2, s2+n2, l2-n2);
	    		 
	      }
	    }

	    static { // register this comparator
	      WritableComparator.define(CoFollowedPairWritable.class, new Comparator());
	    }

	}

