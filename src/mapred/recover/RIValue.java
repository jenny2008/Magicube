package mapred.recover;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class RIValue implements WritableComparable<RIValue> {

	private IntWritable start = new IntWritable();
	private TwoDimmentsionByteArray value = new TwoDimmentsionByteArray();

	public IntWritable getStart() {
		return start;
	}

	public void setStart(IntWritable start) {
		this.start = start;
	}

	public TwoDimmentsionByteArray getValue() {
		return value;
	}

	public void setValue(TwoDimmentsionByteArray value) {
		this.value = value;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.start.readFields(in);
		this.value.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.start.write(out);
		this.value.write(out);
	}

	@Override
	public int compareTo(RIValue o) {
		return this.start.compareTo(o.start);
	}

}
