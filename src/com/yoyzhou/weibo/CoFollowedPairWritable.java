package com.yoyzhou.weibo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;



public class CoFollowedPairWritable implements WritableComparable<CoFollowedPairWritable>{
		

		private long user1;
		private long user2;
		
		public CoFollowedPairWritable(){}
		
		public CoFollowedPairWritable(long usr1, long usr2){
			
			this.set(usr1, usr2);
			
		}
		
		public void set(long usr1, long usr2){
			//make sure the smaller user(uid is smaller) is user1
			if(usr1 <= usr2){
				this.user1 = usr1;
				this.user2 = usr2;
			}else{
				
				this.user1 = usr2;
				this.user2 = usr1;
			}
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			
			out.writeLong(user1);
			out.writeLong(user2);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			
			user1 = in.readLong();
			user2 = in.readLong();
		}

		/** Returns true if <code>o</code> is a CoFollowedPair with the same value. */
		@Override
		  public boolean equals(Object o) {
		    if (!(o instanceof CoFollowedPairWritable))
		    	return false;
		    
		    CoFollowedPairWritable other = (CoFollowedPairWritable)o;
		    return this.user1 == other.user1 && this.user2 == other.user2;
		    
		  }

		@Override
		public String toString() {
			return Long.toString(user1) + "\t" + Long.toString(user2);
		}
		
		/**
		 * Compare two co-followed pairs, return 0 only if this user1 equals that user1 and this user2 equals that user2,
		 * otherwise compare user1 then compare user2. 
		 * */
		@Override
		public int compareTo(CoFollowedPairWritable o) {
			//int cmp = this.user1.compareTo(o.user1);
			if(user1 == o.user1 && user2 == o.user2){
				return 0;
			}else if(user1 <  o.user1){
				return -1;
			}else if(user1 > o.user1){
				return 1;
			}else if(user2 < o.user2){
				return -1;
			}else if(user2 > o.user2){
				return 1;
			}else{
				return 0;
			}
		}
		
		//TODO implements Comparator class for raw byte compare
	}

