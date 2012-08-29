package hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
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
import org.hsqldb.lib.FileUtil;

public class FileInflation {

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
		List<PathTask> paths = new LinkedList<FileInflation.PathTask>();

		File lfile = new File(local);

		recPath(lfile, remote, paths,'/');

		return paths;
	}

	public static void recPath(File local, String currentRemotePath,
			List<PathTask> paths, char combiner) throws IOException {
		if (local.isDirectory()) {
			File[] childrens = local.listFiles();
			for (File f : childrens) {
				recPath(f, currentRemotePath + combiner + f.getName(), paths,'_');
			}
		} else {
			paths.add(new PathTask(local.getAbsolutePath(), currentRemotePath));
		}
	}

	public static void copyFile(String src, String dst) throws IOException {
		byte[] buffer = new byte[1024 * 1024];
		FileInputStream in = new FileInputStream(src);
		FileOutputStream out = new FileOutputStream(dst);

		int length = in.read(buffer);
		while (length > 0) {
			out.write(buffer, 0, length);
			length = in.read(buffer);
		}

		in.close();
		out.close();

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
		
		long start = System.currentTimeMillis();

		System.out.println("start copy");

		for (PathTask pt : paths) {
			System.out.println(pt.from + "," + pt.to);
			copyFile(pt.from, pt.to);
		}

		System.out.println("end copy");

		long end = System.currentTimeMillis();

		System.out.println(new Date() + ":\t" + (end - start));

		System.setOut(out);
	}

}
