package mapred.split;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Logger;

import Mcube.Galios8Num;
import Mcube.Matrix;
import Mcube.MessageSplit;
import cn.ict.cn.dist.DFSUtil;

public class SplitJob {

	public static class SplitMap extends
			Mapper<FileSplit, BytesWritable, IKey, IValue> {

		private int n;
		private int k;
		private Matrix<Galios8Num> EMatrix;

		@Override
		protected void setup(
				org.apache.hadoop.mapreduce.Mapper<FileSplit, BytesWritable, IKey, IValue>.Context context)
				throws IOException, InterruptedException {
			Configuration conf = new Configuration();
			n = conf.getInt("nk.n", 16);
			k = conf.getInt("nk.k", 8);

			try {
				EMatrix = DFSUtil.getE();
			} catch (Exception e) {
				e.printStackTrace();
			}
		};

		@Override
		protected void map(
				FileSplit key,
				BytesWritable value,
				org.apache.hadoop.mapreduce.Mapper<FileSplit, BytesWritable, IKey, IValue>.Context context)
				throws IOException, InterruptedException {
			try {
				String path = key.getPath().toString();

				long start = key.getStart();

				int startRow = (int) (start / k);

				MessageSplit ms = new MessageSplit();
				ms.n = n;
				ms.k = k;

				Matrix<Galios8Num> mg = ms.bytesToMatrix(value.getBytes());

				byte[][] msgp = MessageSplit.messageSplit(mg, EMatrix);

				System.out.println("msgp = " + msgp.length + ","
						+ msgp[0].length);

				for (int i = 0; i < n; ++i) {
					IKey ikey = new IKey();
					ikey.getPath().set(path);
					ikey.getColumn().set(i);
					ikey.getRowStart().set(startRow);

					IValue ivalue = new IValue();
					ivalue.getStart().set(startRow);
					ivalue.getValue().setSize(msgp.length);
					for (int j = 0; j < msgp.length; ++j) {
						ivalue.getValue().getBytes()[j] = msgp[j][i];
					}

					context.write(ikey, ivalue);
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		};
	}

	public static class SplitReduce extends
			Reducer<IKey, IValue, NullWritable, NullWritable> {
		protected void reduce(
				IKey key,
				java.lang.Iterable<IValue> values,
				org.apache.hadoop.mapreduce.Reducer<IKey, IValue, NullWritable, NullWritable>.Context arg2)
				throws IOException, InterruptedException {
			System.out.println(key);

			int[] nkr = DFSUtil.readMeta(key.getPath().toString());
			int r = nkr[2];

			DataOutputStream out = DFSUtil.createSingleBlockFile(key.getPath()
					.toString(), key.getColumn().get(), r);

			// size
			for (IValue value : values) {
				out.write(value.getValue().getBytes(), 0, value.getValue()
						.getLength());
			}
			out.flush();
			out.close();
		};
	}

	public static class SplitPartition extends Partitioner<IKey, IValue> {

		@Override
		public int getPartition(IKey key, IValue value, int numReduceTasks) {

			// System.out.println("partition " + key);

			return ((key.getPath().toString().hashCode() & Integer.MAX_VALUE) * 31 + key
					.getColumn().get()) % numReduceTasks;
		}
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		/* args: path blocksize n k */

		System.out.println(args[0]);

		int blockSize = Integer.parseInt(args[1]);
		int n = Integer.parseInt(args[2]);
		int k = Integer.parseInt(args[3]);

		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost/");
		conf.set("nk.n", n + "");
		conf.set("nk.k", k + "");

		FileSystem fs = FileSystem.get(conf);

		Path p = new Path(args[0]);
		FileStatus stat = fs.getFileStatus(p);

		System.out.println(fs.exists(p));
		int av = (int) stat.getLen();
		System.out.println("av = " + av);

		int r = av / k + ((av % k == 0) ? 0 : 1);
		DFSUtil.writeMeta(args[0], n, k, r);

		Job job = new Job(conf);

		job.setJarByClass(SplitJob.class);

		job.setMapperClass(SplitMap.class);
		job.setReducerClass(SplitReduce.class);
		job.setPartitionerClass(SplitPartition.class);

		job.setNumReduceTasks(16);

		job.setInputFormatClass(BlockFileInputFormat.class);
		job.setOutputFormatClass(NullOutputFormat.class);

		FileInputFormat.addInputPaths(job, args[0]);
		/** block size */
		FileInputFormat.setMinInputSplitSize(job, blockSize);
		FileInputFormat.setMaxInputSplitSize(job, blockSize);

		job.setOutputKeyClass(IKey.class);
		job.setOutputValueClass(IValue.class);

		job.setGroupingComparatorClass(IKey.Comparator.class);
		job.setSortComparatorClass(IKey.KeyComparator.class);

		System.out.println("start");

		job.waitForCompletion(true);
	}
}
