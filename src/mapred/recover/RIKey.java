package mapred.recover;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class RIKey implements WritableComparable<RIKey> {
	static {
		WritableComparator.define(RIKey.class, new Comparator());
	}

	private Text path = new Text();
	private IntWritable rowStart = new IntWritable();

	/*
	 * private IntWritable rowStart = new IntWritable(); private IntWritable
	 * length = new IntWritable();
	 */

	public Text getPath() {
		return path;
	}

	public void setPath(Text path) {
		this.path = path;
	}

	public IntWritable getRowStart() {
		return rowStart;
	}

	public void setRowStart(IntWritable rowStart) {
		this.rowStart = rowStart;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		path.readFields(in);
		rowStart.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		path.write(out);
		rowStart.write(out);
	}

	@Override
	public int compareTo(RIKey another) {
		int cmp = this.path.compareTo(another.path);
		return cmp;
	}

	@Override
	public String toString() {
		return this.getPath().toString() + "," + this.getRowStart().get();
	}

	public static class Comparator extends WritableComparator {
		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
		private static final IntWritable.Comparator INT_COMPARATOR = new IntWritable.Comparator();

		protected Comparator() {
			super(RIKey.class);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				/**
				 * 开始时有一个vint记录Text的长度，所以第一个字段就是VINT本身加其所指的TEXT长度
				 */
				int firstL1 = WritableUtils.decodeVIntSize(b1[s1])
						+ readVInt(b1, s1);
				int firstL2 = WritableUtils.decodeVIntSize(b2[s2])
						+ readVInt(b2, s2);
				int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2,
						firstL2);

				return cmp;
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}
	}

	public static class KeyComparator extends WritableComparator {
		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
		private static final IntWritable.Comparator INT_COMPARATOR = new IntWritable.Comparator();

		protected KeyComparator() {
			super(RIKey.class);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				/**
				 * 开始时有一个vint记录Text的长度，所以第一个字段就是VINT本身加其所指的TEXT长度
				 */
				int firstL1 = WritableUtils.decodeVIntSize(b1[s1])
						+ readVInt(b1, s1);
				int firstL2 = WritableUtils.decodeVIntSize(b2[s2])
						+ readVInt(b2, s2);
				int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2,
						firstL2);
				if (cmp != 0) {
					return cmp;
				}

				cmp = INT_COMPARATOR.compare(b1, s1 + firstL1, 4, b2, s2
						+ firstL2, 4);
				return cmp;
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}
	}

	public static byte[] serialize(RIKey ikey) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutput out = new DataOutputStream(bos);
		ikey.write(out);
		return bos.toByteArray();
	}

	public static RIKey deserialize(byte[] data) throws IOException {
		DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));

		RIKey ori = new RIKey();
		ori.readFields(in);

		return ori;
	}

	public static void main(String[] args) throws IOException {
		RIKey first = new RIKey();
		first.setPath(new Text("/test/res1234"));

		RIKey second = new RIKey();
		second.setPath(new Text("/test/res1234"));

		byte[] d = serialize(first);
		byte[] d2 = serialize(second);

		Comparator c = new Comparator();
		System.out.println(c.compare(d, 0, d.length, d2, 0, d2.length));

		System.out.println(deserialize(d));

	}
}
