package mapred.recover;

import java.io.IOException;

import mapred.split.BlockFileInputFormat;
import mapred.split.IKey;
import mapred.split.IValue;
import mapred.split.SplitJob;
import mapred.split.SplitJob.SplitMap;
import mapred.split.SplitJob.SplitPartition;
import mapred.split.SplitJob.SplitReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import cn.ict.cn.dist.DFSUtil;

import Mcube.Galios8Num;
import Mcube.Matrix;
import Mcube.MessageRec;

public class RecoverJob {

	public static class RecoverMap extends
			Mapper<FileSplit, TwoDimmentsionByteArray, RIKey, RIValue> {

		private Matrix<Galios8Num> EMatrix;

		@Override
		protected void setup(
				org.apache.hadoop.mapreduce.Mapper<FileSplit, TwoDimmentsionByteArray, RIKey, RIValue>.Context context)
				throws java.io.IOException, InterruptedException {
			try {
				EMatrix = DFSUtil.getE();
			} catch (Exception e) {
				e.printStackTrace();
			}
		};

		@Override
		protected void map(
				FileSplit key,
				TwoDimmentsionByteArray value,
				org.apache.hadoop.mapreduce.Mapper<FileSplit, TwoDimmentsionByteArray, RIKey, RIValue>.Context context)
				throws java.io.IOException, InterruptedException {
			try {
				MyFileSplit mfs = (MyFileSplit) key;

				System.out.println(mfs.getPath() + "," + mfs.getStart() + ","
						+ value.getValue().length);

				Matrix<Galios8Num> msgk = MessageRec.genMsgk(value.getValue());

				Matrix<Galios8Num> VEk = MessageRec.genVEk(EMatrix,
						mfs.getSelectedColumn());

				Matrix<Galios8Num> rmsg = MessageRec.getRecMessage(msgk, VEk);

				RIKey ikey = new RIKey();
				ikey.getPath().set(key.getPath().toString());
				ikey.getRowStart().set((int) mfs.getStart());

				RIValue ivalue = new RIValue();
				ivalue.getStart().set((int) mfs.getStart());

				int row = rmsg.getRows();
				int col = rmsg.getCols();

				byte[][] buffer = new byte[row][col];
				for (int i = 0; i < row; ++i) {
					for (int j = 0; j < col; ++j) {
						buffer[i][j] = rmsg.get(i, j).toByte();
					}
				}
				ivalue.getValue().setValue(buffer);

				context.write(ikey, ivalue);

			} catch (Exception e) {
				e.printStackTrace();
			}
		};
	}

	public static class RecoverReduce extends
			Reducer<RIKey, RIValue, NullWritable, NullWritable> {

		protected void reduce(
				RIKey key,
				java.lang.Iterable<RIValue> values,
				org.apache.hadoop.mapreduce.Reducer<RIKey, RIValue, NullWritable, NullWritable>.Context context)
				throws java.io.IOException, InterruptedException {

			System.out.println("reduce...");
			System.out.println(key);

			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataOutputStream out = fs.create(new Path(key.getPath()
					.toString()), true);

			for (RIValue value : values) {
				byte[][] buffer = value.getValue().getValue();
				for (int i = 0; i < buffer.length; ++i) {
					out.write(buffer[i], 0, buffer[i].length);
				}

				System.out.println(value.getStart() + ","
						+ value.getValue().getValue().length);
			}

			out.close();
		};

	}

	public static class RecoverPartition extends Partitioner<RIKey, RIValue> {

		@Override
		public int getPartition(RIKey key, RIValue value, int numReduceTasks) {

			return (key.getPath().toString().hashCode() & Integer.MAX_VALUE)
					% numReduceTasks;
		}

	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		System.out.println(args[0]);
		int blockSize = Integer.parseInt(args[1]);

		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost/");

		Job job = new Job(conf);

		job.setJarByClass(RecoverJob.class);
		job.setMapperClass(RecoverMap.class);
		job.setReducerClass(RecoverReduce.class);
		job.setPartitionerClass(RecoverPartition.class);

		job.setNumReduceTasks(1);

		FragmentInputFormat.setPath(args[0]);
		FragmentInputFormat.setBlockSize(blockSize);

		int a[] = new int[8];
		for (int i = 0; i < a.length; ++i) {
			a[i] = i;
		}
		FragmentInputFormat.setSelectedColumns(a);

		job.setInputFormatClass(FragmentInputFormat.class);
		job.setOutputFormatClass(NullOutputFormat.class);

		job.setOutputKeyClass(RIKey.class);
		job.setOutputValueClass(RIValue.class);

		job.setGroupingComparatorClass(RIKey.Comparator.class);
		job.setSortComparatorClass(RIKey.KeyComparator.class);

		System.out.println("start");

		job.waitForCompletion(true);
	}
}
