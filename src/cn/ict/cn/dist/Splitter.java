package cn.ict.cn.dist;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import Mcube.Galios8Num;
import Mcube.Matrix;
import Mcube.MessageRec;
import Mcube.MessageSplit;

public class Splitter {
	private int n = 16;
	private int k = 8;
	private Matrix<Galios8Num> EMatrix;

	public Splitter() {

	}

	public Splitter(int n, int k) {
		super();
		this.n = n;
		this.k = k;
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

	public Matrix<Galios8Num> getEMatrix() {
		return EMatrix;
	}

	public void setEMatrix(Matrix<Galios8Num> eMatrix) {
		EMatrix = eMatrix;
	}

	public void splitFile(String path) throws Exception {
		FileInputStream in = new FileInputStream(path);

		byte buffer[] = new byte[in.available()];

		// System.out.println(buffer.length);

		in.read(buffer);

		// System.out.println(new String(buffer));

		MessageSplit ms = new MessageSplit();

		Matrix<Galios8Num> mg = ms.bytesToMatrix(buffer);

		// System.out.println(mg.toString());

		// System.out.println(EMatrix.toString());

		byte[][] msgp = MessageSplit.messageSplit(mg, EMatrix);
		// System.out.println(msgp.toString());

		DFSUtil.writeMeta(path, n, k, msgp.length);

		for (int i = 0; i < n; ++i) {
			DFSUtil.saveToSingleBlockFile(path, i, msgp);
		}

	}

	public void splitDir(String path) throws Exception {
		DFSUtil.writeDirInfo(path);

		File dir = new File(path);
		if (dir.exists() && dir.isDirectory()) {
			File files[] = dir.listFiles();

			for (File file : files) {
				if (file.isFile()) {
					System.out.println(file.getName());
					this.splitFile(file.getAbsolutePath());
				}
			}
		} else {
			throw new IOException("Can't open " + path
					+ ", or is not a directory");
		}
	}

	public static void main(String[] args) throws Exception {
		MessageSplit ms = new MessageSplit();

		int n = 16, k = 8;

		Matrix<Galios8Num> eMatrix = ms.genEMatrix(k, n);
		DFSUtil.writeE(eMatrix);

		/*
		 * Splitter splitter = new Splitter(n, k); splitter.setEMatrix(eMatrix);
		 * //splitter.splitFile("/home/owner-pc/test/1.txt");
		 * splitter.splitDir("/home/owner-pc/test");
		 */
	}

}
