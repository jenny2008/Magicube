package cn.ict.cn.dist;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;

import Mcube.Galios8Num;
import Mcube.Matrix;

public class DFSUtil {
	private static final String BASE_PATH = "/nk/";

	public static DFSClient client;

	private static Configuration conf;
	static {
		conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost/");
		try {
			client = new DFSClient(conf);

			if (!client.exists(BASE_PATH)) {
				client.mkdirs(BASE_PATH);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * File size must be divided by checksum size
	 * 
	 * @param size
	 * @return
	 */
	private static int getFilledSize(int size) {
		int checksum = conf.getInt("io.bytes.per.checksum", -1);
		// System.out.println("checksum = " + checksum);

		size += checksum - size % checksum;
		// System.out.println("size = " + size);

		return size;
	}

	private static String dealWithPathDir(String path) throws IOException {
		int commaPos = path.indexOf(':');
		if (commaPos != -1) {
			int slashPos = path.indexOf('/', commaPos + 3);
			path = path.substring(slashPos + 1);
		}

		int slashLastPos = path.lastIndexOf('/');
		if (slashLastPos != -1) {
			String oriDir = path.substring(0, slashLastPos);
			String aimDir = BASE_PATH + oriDir;
			if (!client.exists(aimDir)) {
				client.mkdirs(aimDir);
			}
		}

		if (path.startsWith("/")) {
			return path.substring(1);
		}
		return path;
	}

	public static void writeMeta(String path, int n, int k, int r)
			throws IOException {
		path = dealWithPathDir(path);

		String storePath = BASE_PATH + path + "_meta";

		int size = getFilledSize(4 * 3);

		DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
				client.create(storePath, true, (short) 1, size)));

		out.writeInt(n);
		out.writeInt(k);
		out.writeInt(r);

		out.close();
	}

	// n,k,r
	public static int[] readMeta(String path) throws IOException {
		String storePath = BASE_PATH + dealWithPathDir(path) + "_meta";

		DataInputStream in = new DataInputStream(client.open(storePath));

		int[] result = new int[3];

		for (int i = 0; i < result.length; ++i) {
			result[i] = in.readInt();
		}

		in.close();
		return result;
	}

	public static void readSingleBlock(String path, int startRow, int col,
			int newRow, byte[][] value) throws Exception {
		String storePath = BASE_PATH + dealWithPathDir(path) + "_" + col;
		
		DataInputStream in = new DataInputStream(client.open(storePath));
		in.skip(startRow);

		for (int i = 0; i < value[0].length; ++i) {
			value[newRow][i] = in.readByte();
		}
		
		System.out.println(storePath + "," + value);

		in.close();
	}

	/**
	 * 注意矩阵是转置的
	 * 
	 * @param path
	 * @param col
	 * @param newRow
	 * @param value
	 * @throws Exception
	 */
	public static void readSingleBlock(String path, int col, int newRow,
			byte[][] value) throws Exception {
		String storePath = BASE_PATH + dealWithPathDir(path) + "_" + col;

		DataInputStream in = new DataInputStream(client.open(storePath));

		for (int i = 0; i < value[0].length; ++i) {
			value[newRow][i] = in.readByte();
		}

		in.close();
	}

	public static DataOutputStream createSingleBlockFile(String path, int col,
			int size) throws IOException {
		path = dealWithPathDir(path);

		String storePath = BASE_PATH + path + "_" + col;

		size = getFilledSize(size);

		System.out.println("size = " + size);

		DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
				client.create(storePath, true, (short) 1, size)));

		return out;
	}

	public static void saveToSingleBlockFile(String path, int col,
			byte[][] values) throws IOException {
		saveToSingleBlockFile(path, col, col, values);
	}

	public static void saveToSingleBlockFile(String path, int col, int realCol,
			byte[][] values) throws IOException {
		path = dealWithPathDir(path);

		String storePath = BASE_PATH + path + "_" + realCol;

		int size = getFilledSize(values.length);

		DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
				client.create(storePath, true, (short) 1, size)));

		for (int i = 0; i < values.length; ++i) {
			out.writeByte(values[i][col]);
		}

		out.close();
	}

	public static void writeE(Matrix<Galios8Num> matrix) throws IOException {
		String storePath = BASE_PATH + "E";

		int row = matrix.getRows();
		int col = matrix.getCols();

		int size = getFilledSize(row * col + 2 * 4);

		DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
				client.create(storePath, true, (short) 1, size)));

		out.writeInt(matrix.getRows());
		out.writeInt(matrix.getCols());
		for (int i = 0; i < row; ++i) {
			for (int j = 0; j < col; ++j) {
				out.writeByte(matrix.get(i, j).toByte());
			}
		}

		out.close();
	}

	public static Matrix<Galios8Num> getE() throws Exception {
		String storePath = BASE_PATH + "E";

		DataInputStream in = new DataInputStream(client.open(storePath));
		int row = in.readInt();
		int col = in.readInt();

		Matrix<Galios8Num> matrix = new Matrix<Galios8Num>(row, col,
				Galios8Num.getOne(), Galios8Num.getZero());

		for (int i = 0; i < row; ++i) {
			for (int j = 0; j < col; ++j) {
				byte b = in.readByte();
				matrix.set(i, j, Galios8Num.getNum(b));
			}
		}
		in.close();

		return matrix;
	}

	public static void writeMatrixToFile(String hdfsPath,
			Matrix<Galios8Num> matrix) throws IOException {
		DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
				client.create(hdfsPath, true)));

		for (int i = 0; i < matrix.getRows(); ++i) {
			for (int j = 0; j < matrix.getCols(); ++j) {
				out.writeByte(matrix.get(i, j).toByte());
			}
		}

		out.close();
	}

	public static String[] readDirInfo(String path) throws Exception {
		String storePath = BASE_PATH + dealWithPathDir(path) + "_dir";

		DataInputStream in = new DataInputStream(client.open(storePath));

		String filePath = in.readUTF();

		in.close();

		return filePath.split(",");
	}

	public static void writeDirInfo(String path) throws Exception {
		File dir = new File(path);

		path = dealWithPathDir(path);

		String storePath = BASE_PATH + path + "_dir";

		if (dir.exists() && dir.isDirectory()) {
			DataOutputStream out = new DataOutputStream(
					new BufferedOutputStream(client.create(storePath, true)));

			String filePaths = "";

			File[] files = dir.listFiles();
			for (File f : files) {
				if (f.isFile()) {
					filePaths += f.getName() + ",";
				}
			}

			out.writeUTF(filePaths);
			out.close();
		} else {
			throw new IOException();
		}
	}
}
