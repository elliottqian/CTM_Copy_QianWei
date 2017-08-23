package mainpage_log;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LongPair implements WritableComparable<LongPair> {
	private long first = 0;
	private long second = 0;

	public String toString() {
		return first + "\t" + second;
	}

	/**
	 * Set the left and right values.
	 */
	public void set(long left, long right) {
		first = left;
		second = right;
	}

	public long getFirst() {
		return first;
	}

	public long getSecond() {
		return second;
	}

	/**
	 * Read the two integers. Encoded as: MIN_VALUE -> 0, 0 -> -MIN_VALUE,
	 * MAX_VALUE-> -1
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		//first = in.readInt() + Integer.MIN_VALUE;
		//second = in.readInt() + Integer.MIN_VALUE;
		
		first = in.readLong() + Long.MIN_VALUE;
		second = in.readLong() + Long.MIN_VALUE;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		//out.writeInt(first - Integer.MIN_VALUE);
		//out.writeInt(second - Integer.MIN_VALUE);
		out.writeLong(first - Long.MIN_VALUE);
		out.writeLong(second - Long.MIN_VALUE);
	}

	@Override
	public int hashCode() {
		return (int) (first * 157 + second);
	}

	@Override
	public boolean equals(Object right) {
		if (right instanceof LongPair) {
			LongPair r = (LongPair) right;
			return r.first == first && r.second == second;
		} else {
			return false;
		}
	}

	/** A Comparator that compares serialized IntPair. */
	public static class Comparator extends WritableComparator {
		public Comparator() {
			super(LongPair.class);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return compareBytes(b1, s1, l1, b2, s2, l2);
		}
	}

	static { // register this comparator
		WritableComparator.define(LongPair.class, new Comparator());
	}

	@Override
	public int compareTo(LongPair o) {
		if (first != o.first) {
			return first < o.first ? -1 : 1;
		} else if (second != o.second) {
			return second < o.second ? -1 : 1;
		} else {
			return 0;
		}
	}

	public static int compare(long left, long right) {
		// TODO Auto-generated method stub
		return left > right ? 1 : (left == right ? 0 : -1);
	}

	/*use to get Partition, Partition ��Ӧreduce */
	public static class FirstPartitioner extends
            Partitioner<LongPair, Text> {

		@Override
		public int getPartition(LongPair key, Text value,
                                int numPartitions) {
			return (int) (Math.abs(key.getFirst() * 127) % numPartitions);
		}
	}

	/*used to get group,group��Ӧһ�����*/
	public static class FirstGroupingComparator implements
            RawComparator<LongPair> {
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return WritableComparator.compareBytes(b1, s1, Long.SIZE / 8,
					b2, s2, Long.SIZE / 8);
		}

		@Override
		public int compare(LongPair o1, LongPair o2) {
			long l = o1.getFirst();
			long r = o2.getFirst();
			return l == r ? 0 : (l < r ? -1 : 1);
		}
	}
	
	/*���ڶ�key��������*/
	public static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
			super(LongPair.class, true);
			// TODO Auto-generated constructor stub
		}

		@Override
		public int compare(WritableComparable o1, WritableComparable o2) {
			// TODO Auto-generated method stub
			LongPair ip1 = (LongPair) o1;
			LongPair ip2 = (LongPair) o2;
			int cmp = LongPair.compare(ip1.getFirst(), ip2.getFirst());
			if (cmp != 0)
				return cmp;
			return LongPair.compare(ip1.getSecond(), ip2.getSecond());
		}

	}

}