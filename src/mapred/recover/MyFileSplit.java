package mapred.recover;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MyFileSplit extends FileSplit {
	private int[] selectedColumn;
	private int n;
	private int k;
	private int r;

	MyFileSplit() {
		super(new Path("file:///"), 0, 0, null);
	}

	MyFileSplit(Path path, long start, long length, String[] hosts) {
		super(path, start, length, hosts);
	}

	public int[] getSelectedColumn() {
		return selectedColumn;
	}

	public void setSelectedColumn(int[] selectedColumn) {
		this.selectedColumn = new int[selectedColumn.length];
		for (int i = 0; i < selectedColumn.length; ++i) {
			this.selectedColumn[i] = selectedColumn[i];
		}
	}

	public int getN() {
		return n;
	}

	public void setN(int n) {
		this.n = n;
	}

	public int getK() {
		return k;
	}

	public void setK(int k) {
		this.k = k;
	}

	public int getR() {
		return r;
	}

	public void setR(int r) {
		this.r = r;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		super.readFields(in);
		
		this.n = in.readInt();
		k = in.readInt();
		r = in.readInt();
		
		this.selectedColumn = new int[in.readInt()];
		for(int i = 0;i < selectedColumn.length;++i){
			this.selectedColumn[i] = in.readInt();
		}
				
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		super.write(out);
		
		out.writeInt(n);
		out.writeInt(k);
		out.writeInt(r);
		
		out.writeInt(this.selectedColumn.length);
		for(int i = 0;i < this.selectedColumn.length;++i){
			out.writeInt(this.selectedColumn[i]);
		}
	}

}
