package Mcube;

import Mcube.Rational;
import Mcube.Galios8Num;
import Mcube.Number;
import Mcube.Matrix;

public class MessageSplit {

	/* notice:
	 * set the value of n and k, this can be changed!
	 * 
	 */
	public int n = 16, k = 8;
	
	// transform a message into a Matrix
	public Matrix<Galios8Num> messageToMatrix(String msg) throws Exception {
		int l = msg.length();
		int r = (l + (k - 1)) / k;   // "r" is the rows of the Message Matrix
		
		//Galios8Num [][] mg = new Galios8Num[r][k];
		
		// "mg" is the Matrix expression for a message
		Matrix<Galios8Num> mg = new Matrix<Galios8Num> (r, k, Galios8Num.getOne(), 
				Galios8Num.getZero());
		
		byte[] m = msg.getBytes();
		int q = 0;
		while (q < m.length) {
			for (int i = 0; i < r; i++) {
				for (int j = 0; j < k; j++) {
					if (q < m.length) {
						mg.set(i, j, Galios8Num.getNum(m[q]));
						q++;
					}
				} 
			}
		}
		return mg;	
	}
	
	public Matrix<Galios8Num> bytesToMatrix(byte m[]) throws Exception {
		int l = m.length;
		int r = (l + (k - 1)) / k;   // "r" is the rows of the Message Matrix
		
		//Galios8Num [][] mg = new Galios8Num[r][k];
		
		// "mg" is the Matrix expression for a message
		Matrix<Galios8Num> mg = new Matrix<Galios8Num> (r, k, Galios8Num.getOne(), 
				Galios8Num.getZero());
		
		int q = 0;
		while (q < m.length) {
			for (int i = 0; i < r; i++) {
				for (int j = 0; j < k; j++) {
					if (q < m.length) {
						mg.set(i, j, Galios8Num.getNum(m[q]));
						q++;
					}
				} 
			}
		}
		return mg;	
	}
	
	// generate a E Matrix for Message split
	public static Matrix<Galios8Num> genEMatrix (int k, int n) throws Exception {
		Matrix<Galios8Num> e = new Matrix<Galios8Num> (k, n, Galios8Num.getOne(), 
				Galios8Num.getZero());		
		
		Galios8Num[] A = Galios8Num.getAllGalios8Nums();
		shuffle(A);
		
		for(int i=0; i<n; i++) {
			
			for(int j=0; j<k; j++) {	
				Galios8Num tmp = A[i];
				for (int q=1; q<j+1; q++) {
					tmp = tmp.mul(A[i]);
				}
				e.set(j, i, tmp);
			  
		//		EMatrix[j][i] = (int)Galios8Num.pow(A[i],(j+1));
			//	EMatrix[j][i] = (int)Math.pow(A[i],(j+1));
		//		EMatrix[j][i] = EMatrix[j][i] % 255;
			}
		}
				
		return e;
		
	}
	
	// encode and split message, each column of the result Matrix is a split of the source message.
	public static byte[][] messageSplit(Matrix<Galios8Num> mg, Matrix<Galios8Num> EMatrix) throws Exception {
		Matrix<Galios8Num> sp = mg.mul(EMatrix);
		
		int rows = sp.getRows();
		int cols = sp.getCols();
		byte[][] spm = new byte[rows][cols];
		
		for (int i = 0; i < rows; i++) {
			for (int j = 0; j < cols; j++) {
				spm[i][j] = sp.get(i, j).toByte();
			}
		}
		
		return spm;
	}
	
	public static Galios8Num[] shuffle(Galios8Num[] a) {
		// for 1:n swap(a[i], a[random(i, n)])			
			for(int i=0;i<a.length;i++) {
				Rational.swap(a, i, Rational.random(i, a.length));			
			}
			
			return a;
	}
	
	public static void swap(Galios8Num[] a, int i, int n) {
		Galios8Num temp = a[i];	    
		a[i] = a[n];
		a[n] = temp;
	}
	
	public static void main(String[] args) throws Exception {
		String str = "hello world!";
		MessageSplit ms = new MessageSplit();
		
		int n = 16, k = 8;
		
		System.out.println(str);
		Matrix<Galios8Num> mg = ms.messageToMatrix(str);
				
		System.out.println(mg.toString());
		
		Matrix<Galios8Num> EMatrix = ms.genEMatrix(k, n);
		System.out.println(EMatrix.toString());
		
		byte[][] msgp = messageSplit(mg, EMatrix);
		System.out.println(msgp.toString());
		System.out.println(msgp.length + "," + msgp[0].length);
		
		int[] A = {0,1,2,3,4,5,6,7};
		byte[][] data = new byte[k][msgp.length];
		for (int i = 0; i < k; i++) {
			for (int j = 0; j < msgp.length; j++) {
				data[i][j] = msgp[j][i];
			} 
		}
		
		System.out.println(data.length + "," + data[0].length);
		
		Matrix<Galios8Num> msgk = MessageRec.genMsgk(data);
		Matrix<Galios8Num> VEk = MessageRec.genVEk(EMatrix, A);
		
		Matrix<Galios8Num> rmsg = MessageRec.getRecMessage(msgk, VEk);
		
		System.out.println(rmsg.toString());
		
	}
}
