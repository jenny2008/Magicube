package mapred.recover;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import cn.ict.cn.dist.DFSUtil;

public class FragmentInputFormat extends
		InputFormat<FileSplit, TwoDimmentsionByteArray> {

	private static String path;
	private static int blockSize = 1024 * 1024 * 64;
	private static int[] selectedColumns;

	public static String getPath() {
		return path;
	}

	public static void setPath(String path) {
		FragmentInputFormat.path = path;
	}

	public static int getBlockSize() {
		return blockSize;
	}

	public static int[] getSelectedColumns() {
		return selectedColumns;
	}

	public static void setSelectedColumns(int[] selectedColumns) {
		FragmentInputFormat.selectedColumns = selectedColumns;
	}

	public static void setBlockSize(int blockSize) {
		FragmentInputFormat.blockSize = blockSize;
	}

	@Override
	public RecordReader<FileSplit, TwoDimmentsionByteArray> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException {
		return new FragmentRecordReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException,
			InterruptedException {
		if (path == null) {
			throw new IOException("Path is null!");
		}

		int[] nkr = DFSUtil.readMeta(path);
		int n = nkr[0];
		int k = nkr[1];
		int r = nkr[2];

		if (selectedColumns == null || selectedColumns.length != k) {
			throw new IOException("selected column is not " + k);
		}

		int oneSplitRows = blockSize / k;

		List<InputSplit> result = new LinkedList<InputSplit>();

		for (int i = 0; i < r; i += oneSplitRows) {
			int length = Math.min(oneSplitRows, r - i);
			MyFileSplit split = new MyFileSplit(new Path(path), i, length, null);
			split.setSelectedColumn(selectedColumns);
			split.setN(n);
			split.setK(k);
			split.setR(r);
			result.add(split);
			
			System.out.println(split);
		}

		return result;
	}
}
