package mapred.recover;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TwoDimmentsionByteArray implements
		WritableComparable<TwoDimmentsionByteArray> {

	private byte[][] value;

	public byte[][] getValue() {
		return value;
	}

	public void setValue(byte[][] value) {
		this.value = value;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int row = in.readInt();
		value = new byte[row][];
		for (int i = 0; i < row; ++i) {
			int col = in.readInt();
			value[i] = new byte[col];
			for (int j = 0; j < col; ++j) {
				value[i][j] = in.readByte();
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		int length = value.length;
		out.writeInt(length);

		for (int i = 0; i < length; ++i) {
			int col = value[i].length;
			out.writeInt(col);

			for (int j = 0; j < col; ++j) {
				out.writeByte(value[i][j]);
			}
		}
	}

	@Override
	public int compareTo(TwoDimmentsionByteArray other) {
		// TODO Auto-generated method stub
		return 0;
	}

	public static byte[] serialize(TwoDimmentsionByteArray tba) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutput out = new DataOutputStream(bos);
		tba.write(out);
		return bos.toByteArray();
	}

	public static TwoDimmentsionByteArray deserialize(byte[] data) throws IOException {
		DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
		
		TwoDimmentsionByteArray tba = new TwoDimmentsionByteArray();
		tba.readFields(in);

		return tba;
	}

	public static void main(String[] args) throws IOException {
		TwoDimmentsionByteArray tba = new TwoDimmentsionByteArray();
		byte b[][] = new byte[16][16];
		for(int i = 0;i < b.length;++i){
			for(int j = 0;j < b[0].length;++j){
				b[i][j] = (byte) (i + j);
			}
		}
		
		tba.setValue(b);
		
		byte[] re = serialize(tba);
		TwoDimmentsionByteArray tba2 = deserialize(re);
		System.out.println(tba2.value.length);
	}
}
