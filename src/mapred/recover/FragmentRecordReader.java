package mapred.recover;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.record.Record;

import cn.ict.cn.dist.DFSUtil;

import com.google.protobuf.UnknownFieldSet.Field;

public class FragmentRecordReader extends
		RecordReader<FileSplit, TwoDimmentsionByteArray> {

	private FileSplit key;
	private TwoDimmentsionByteArray value;
	private boolean firstReaded;

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public FileSplit getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public TwoDimmentsionByteArray getCurrentValue() throws IOException,
			InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (firstReaded) {
			return 1;
		}
		return 0;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		System.out.println(split);
		
		MyFileSplit fileSplit = (MyFileSplit) split;

		int[] sc = fileSplit.getSelectedColumn();
		int k = fileSplit.getK();
		int length = (int) fileSplit.getLength();
		
		System.out.println(fileSplit.getN() + "," + fileSplit.getR());
		System.out.println("k = " + k + "," + "length = " + length);
		
		byte[][] buffer = new byte[k][length];
		
		for (int i = 0; i < k; ++i) {
			try {
				DFSUtil.readSingleBlock(fileSplit.getPath().toString(),
						(int) fileSplit.getStart(), i, sc[i], buffer);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		value = new TwoDimmentsionByteArray();
		value.setValue(buffer);
		key = fileSplit;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!firstReaded) {
			firstReaded = true;
			return true;
		}
		return false;
	}

}
