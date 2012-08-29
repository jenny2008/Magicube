package hdfs;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSReadTest {

	private static FileSystem fs;
	static {
		Configuration conf = new Configuration();
		try {
			fs = FileSystem.get(URI.create("hdfs://gb29:54310/"), conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	static class PathTask {
		String from;
		String to;

		public PathTask(String from, String to) {
			super();
			this.from = from;
			this.to = to;
		}
	}

	public static List<PathTask> generatePathList(String local, String remote)
			throws IOException {
		List<PathTask> paths = new LinkedList<HDFSReadTest.PathTask>();

		File lfile = new File(local);

		recPath(lfile, remote, paths);

		return paths;
	}

	public static void recPath(File local, String currentRemotePath,
			List<PathTask> paths) throws IOException {
		if (local.isDirectory()) {

			// create dir structure first
			fs.mkdirs(new Path(currentRemotePath));
			//System.out.println("making dir " + currentRemotePath + "...");

			File[] childrens = local.listFiles();
			for (File f : childrens) {
				recPath(f, currentRemotePath + "/" + f.getName(), paths);
			}
		} else {
			paths.add(new PathTask(local.getAbsolutePath(), currentRemotePath));
		}
	}

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.out.println("XX local remote");
			System.exit(1);
		}

		PrintStream nout = new PrintStream(new File("result.txt"));
		PrintStream out = System.out;
		System.setOut(nout);

		String localDir = args[0];
		String remoteDir = args[1];

		System.out.println(localDir + " to " + remoteDir);

		List<PathTask> paths = generatePathList(localDir, remoteDir);
		
		System.out.println(paths.size());

		long start = System.currentTimeMillis();

		System.out.println("start copy");
		
		for(PathTask pt : paths){
			fs.copyFromLocalFile(new Path(pt.from), new Path(pt.to));
		}
		
		System.out.println("end copy");
		

		long end = System.currentTimeMillis();

		System.out.println(new Date() + ":\t" + (end - start));

		System.setOut(out);
	}

}
