package Mcube;

import Mcube.Galios8Num.GaliosNumOutOfBoundException;

public class MessageRec {

	public static Matrix<Galios8Num> genMsgk(byte[][] data) throws Exception {
		int rows = data[0].length; //rows is the row number of the Msgk.
		int cols = data.length;    //cols is the column number of the Msgk.
		Matrix<Galios8Num> Msgk = new Matrix<Galios8Num> (rows, cols, Galios8Num.getOne(), Galios8Num.getZero());
	    
		for (int i = 0; i < rows; i++) {
			for (int j = 0; j < cols; j++) {
				Msgk.set(i, j, Galios8Num.getNum(data[j][i]));
			}
		}
		return Msgk;
	}
	
	// generate invertible matrix Ek for reconstruct message.
	public static Matrix<Galios8Num> genVEk(Matrix<Galios8Num> EMatrix, int[] A) throws Exception {
		int k = EMatrix.getRows();
		Matrix<Galios8Num> Ek = new Matrix<Galios8Num> (k, k, Galios8Num.getOne(), Galios8Num.getZero());
		
		for (int i = 0; i < A.length; i++) {
			for (int j = 0; j < k ; j++) {
				Ek.set(j, i, EMatrix.get(j, A[i]));
			}
		}
		
		Matrix<Galios8Num> VEk = Ek.getVer();
		
		return VEk;
	}
	
	// reconstruct message
	public static Matrix<Galios8Num> getRecMessage(Matrix<Galios8Num> Msgk, Matrix<Galios8Num> VEk) throws Exception {
		Matrix<Galios8Num> rmsg = Msgk.mul(VEk);
		
		return rmsg;
	}
	
	
	/*
	// transform each byte[] into a column vector of messagek Matrix for reconstruct the source message.
	public static Vector<Galios8Num> getVector(byte[] msgp) throws GaliosNumOutOfBoundException {
		Vector<Galios8Num> v = new Vector(msgp.length, Galios8Num.getOne(), Galios8Num.getZero());
		
		for (int i = 0; i < msgp.length; i++) {
			v.set(i, Galios8Num.getNum(msgp[i]));
		}
		
		return v;
	}
	*/
}
