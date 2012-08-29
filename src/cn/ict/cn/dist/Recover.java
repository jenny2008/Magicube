package cn.ict.cn.dist;

import java.io.FileInputStream;
import java.io.IOException;

import Mcube.Galios8Num;
import Mcube.Matrix;
import Mcube.MessageRec;
import Mcube.MessageSplit;

public class Recover {
	private static Matrix<Galios8Num> EMatrix;
	static {
		try {
			EMatrix = DFSUtil.getE();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void recoverFile(String path, String hdfsPath) throws Exception {
		System.out.println(path + "," + hdfsPath);
		int[] nkr = DFSUtil.readMeta(path);
		int n = nkr[0];
		int k = nkr[1];
		int r = nkr[2];

		System.out.printf("n = %d,k = %d,r = %d\n", n, k, r);

		int[] a = new int[k];
		for (int i = 0; i < k; ++i) {
			a[i] = i;
		}

		byte[][] data = new byte[k][r];
		for (int i = 0; i < k; i++) {
			DFSUtil.readSingleBlock(path, a[i], i, data);
		}

		Matrix<Galios8Num> msgk = MessageRec.genMsgk(data);

		Matrix<Galios8Num> VEk = MessageRec.genVEk(EMatrix, a);

		Matrix<Galios8Num> rmsg = MessageRec.getRecMessage(msgk, VEk);

		DFSUtil.writeMatrixToFile(hdfsPath, rmsg);

		int row = rmsg.getRows();
		int column = rmsg.getCols();

		System.out.println(row + "," + column);

		byte result[] = new byte[row * column];
		for (int i = 0; i < row; ++i) {
			for (int j = 0; j < column; ++j) {
				result[i * column + j] = rmsg.get(i, j).toByte();
			}
		}

		System.out.println("********");
		System.out.println(new String(result));

		System.out.println(rmsg.toString());
	}

	public void recoverDir(String path, String hdfsPath) throws Exception {
		if (DFSUtil.client.exists(hdfsPath)) {
			DFSUtil.client.delete(hdfsPath, true);
		}

		DFSUtil.client.mkdirs(hdfsPath);

		String sub[] = DFSUtil.readDirInfo(path);

		for (String s : sub) {
			System.out.println(s);
			this.recoverFile(path + "/" + s, hdfsPath + "/" + s);
		}
	}

	public static void main(String[] args) throws Exception {
		Recover r = new Recover();
		r.recoverFile("/input/result/result.txt", "/result/result.txt");
		//r.recoverDir("/input/result", "/result2");
	}

}
