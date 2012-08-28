package mapred.split;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class BlockFileRecordReader extends
		RecordReader<FileSplit, BytesWritable> {
	private FileSplit key;
	private BytesWritable value;
	private boolean firstReaded = false;

	@Override
	public void close() throws IOException {

	}

	@Override
	public FileSplit getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException,
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
		FileSplit fsplit = (FileSplit) split;

		key = fsplit;

		Path file = fsplit.getPath();

		FSDataInputStream in = file.getFileSystem(context.getConfiguration())
				.open(file);
		in.seek(fsplit.getStart());

		int totalLength = (int) fsplit.getLength();

		value = new BytesWritable();
		value.setCapacity(totalLength);

		in.read(value.getBytes(), 0, totalLength);

		System.out.println(fsplit.getPath() + "," + fsplit.getStart());

		in.close();
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
