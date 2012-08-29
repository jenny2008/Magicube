package hdfs;

import java.io.File;
import java.io.FileInputStream;
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

public class FileCombiner {
	private static final long G2 = 1024 * 1024 * 512L;

	private static FileOutputStream out;
	private static byte[] buffer = new byte[1024 * 1024];
	private static long count = 0;
	private static long fileCount = 0;
	private static String filePath;

	public static void combineFile(String srcPath) throws IOException {
		File src = new File(srcPath);
		
		recPath(src);
	}

	public static void recPath(File local) throws IOException {
		if (local.isDirectory()) {
			File[] childrens = local.listFiles();
			for (File f : childrens) {
				recPath(f);
			}
		} else {
			//System.out.println("combining " + local.getAbsolutePath() + ",...");
			FileInputStream in = new FileInputStream(local);
			int length = in.read(buffer);
			while (length > 0) {
				count += length;
				out.write(buffer, 0, length);
				length = in.read(buffer);
			}
			in.close();
			if (count > G2) {
				System.out.println("count = " + count);
				//split
				out.close();
				out = new FileOutputStream(filePath + "_" + fileCount);
				++fileCount;
				count = 0;
			}
			//System.out.println("combined " + local.getAbsolutePath() + ",ok");
		}
	}

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.out.println("XX local remote");
			System.exit(1);
		}

		String localDir = args[0];
		String dst = args[1];
		
		filePath = dst;
		
		out = new FileOutputStream(dst);

		combineFile(localDir);

		out.close();
	}

}
