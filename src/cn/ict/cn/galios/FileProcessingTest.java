package cn.ict.cn.galios;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

public class FileProcessingTest {
	public static void main(String[] args) throws Exception {
		long t0 = System.currentTimeMillis();
		File fin = new File("/tmp/1.txt");
		Matrix<Galios8Num> fileMat = fileToMatrix(fin, 50);
		//System.out.println("fileMat: ");
		//System.out.println(fileMat.toString());
		
		Matrix<Galios8Num> EMatrix = genEMatrix(50, 100);
		
		System.out.println("EMatrix: ");
//		System.out.println(EMatrix.toString());
		
		
		long t1 = System.currentTimeMillis();
		Matrix<Galios8Num> File = fileSplit(fileMat, EMatrix);
		long t2 = System.currentTimeMillis();
		
		//System.out.println("File: ");
//		System.out.println(File.toString());

		
	//	int[] A = getKchunks(4, 10);
		int[] A = {1, 3, 5, 7, 9, 11 ,13, 15, 17, 19,
				   21, 23, 25, 27, 29, 31, 33, 35, 37, 39,
				   41, 43, 45, 47, 49, 51, 53, 55, 57, 59,
				   61, 63, 65, 67, 69, 71, 73, 75, 77, 79,
				   81, 83, 85, 87, 89, 91, 93, 95, 97, 99};

		
		Matrix<Galios8Num> Ek = getEk(A, EMatrix);
		Matrix<Galios8Num> Fk = getFk(A, File);
		Matrix<Galios8Num> Dfile = getDfile(Fk, Ek);
		
		System.out.println("fileMat: ");
		//System.out.println(fileMat.toString());
		System.out.println("Dfile: ");
		//System.out.println(Dfile.toString());
		
		/*
		for (int i = 0; i < fileMat.getRows(); i++ ) {
			for (int j = 0; j< fileMat.getCols(); j++) {
				if (fileMat.get(i, j) != Dfile.get(i, j)) {
					System.out.println(String.format("file[i][j] Dfile[i][j] %d %d %d %d", 
							i, j, fileMat.get(i, j).toInt(), Dfile.get(i, j).toInt()));
				}
			}
		}*/
		
		//System.out.println(fileMat.toString());
		
		long t3 = System.currentTimeMillis();
		FileOutputStream fw = new FileOutputStream("/tmp/re01.txt");
		writeToFile(Dfile, fw);
		fw.close();
	//	System.out.println(fileMat.toString());
		System.out.format("read file: %d\n", t1 - t0);
		System.out.format("split file: %d\n", t2 - t1);
		System.out.format("reconstruct file: %d\n", t3 - t2);
	}
	
	public static Matrix<Galios8Num> fileToMatrix (File file, int cols) throws Exception {
		int rows = ((int)file.length() + cols - 1) / cols;
		
		Matrix<Galios8Num> m = new Matrix<Galios8Num>(rows, cols,
				Galios8Num.getOne(), Galios8Num.getZero());
		
		//BufferedReader bf = new BufferedReader(new FileReader(file));
		/* FIXME: using binary mode */
		FileInputStream fis = new FileInputStream(file);
		try {
			for (int i = 0; i < rows; i++ ) {
				for (int j = 0; j < cols; j++) {
					int in = fis.read();
					if (in != -1) {
						if (in < 0 || in > 255) {
							throw new Exception(
									String.format("elements out of range 0x%x ", in));
						}
						m.set(i, j, Galios8Num.getNum(in));
					}
				}
			}
		} finally {
			fis.close();
		}

		return m;
	}
	
	// this function is used for splitting file into n chunks
	public static Matrix<Galios8Num> fileSplit (Matrix<Galios8Num> File, 
			Matrix<Galios8Num> EMatrix) throws Exception {
		
		Matrix<Galios8Num> chunks = File.mul(EMatrix);
		
		return chunks;
	}
	
	// restore File using k chunks and the corresponding EMatrix
	public static Matrix<Galios8Num> getDfile (Matrix<Galios8Num> kChunks, Matrix<Galios8Num> Ek) throws Exception {
//		System.out.println(Ek.toString());
//		System.out.println(Ek.mul(Ek.getVer()));
		Matrix<Galios8Num> Dfile = kChunks.mul(Ek.getVer());
		return Dfile;
	} 
	
	// get the Ek Matrix corresponding A[i] cols
	public static  Matrix<Galios8Num> getEk (int[] A, Matrix<Galios8Num> En) throws Exception {
		Matrix<Galios8Num> Ek = new Matrix<Galios8Num> (En.getRows(), A.length,
				Galios8Num.getOne(),
				Galios8Num.getZero());
		for (int i = 0; i < A.length; i++) {
			for ( int j = 0; j < En.getRows(); j++) {
				Ek.set(j, i, En.get(j, A[i])); 
			}
		}
		
		return Ek;
	}
	
	// get k chunks of file for restoring file
	public static  Matrix<Galios8Num> getFk (int[] A, Matrix<Galios8Num> File) throws Exception {
		Matrix<Galios8Num> Fk = new Matrix<Galios8Num> (File.getRows(), A.length,
				Galios8Num.getOne(),
				Galios8Num.getZero());
		for (int i = 0; i < A.length; i++) {
			for ( int j = 0; j < File.getRows(); j++) {
				Fk.set(j, i, File.get(j, A[i])); 
			}
		}
		
		return Fk;
	}
	
	public static void writeToFile (Matrix<Galios8Num> Dfile, FileOutputStream fw) throws IOException {
		for(int i = 0; i < Dfile.getRows(); i++) {
			for(int j =0 ; j < Dfile.getCols(); j++) {
				fw.write(Dfile.get(i, j).toInt());
			} 
		}
	}
	
	// generate a E Matrix for File split
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
	
	public static int[] getKchunks(int k, int n) {
		//int[] A = inputColNumber(k,n);
		int[] A = new int[4];
		A[0] = 2;
		A[1] = 5;
		A[2] = 7;
		A[3] = 9;
		
		return A;
	}
	
	public static int[] inputColNumber(int k, int n) {
		int[] cN = new int[k];
		
		System.out.println("Please input k numbers between 0 and n:");
		Scanner scanner = new Scanner(System.in);
		for(int i=0; i<k; i++) {
			cN[i] = scanner.nextInt();
			while(cN[i] < 0 || cN[i] > n) {
				System.out.println("the number you've input is out of range, please retype another number !");
				cN[i] = scanner.nextInt();
			}
		}
		scanner.close();
		return cN;
	}
	
	public static Galios8Num[] shuffle(Galios8Num[] a) {
		// for 1:n swap(a[i], a[random(i, n)])			
			for(int i=0;i<a.length;i++) {
				Rational.swap(a, i, Rational.random(i, a.length));			
			}
			
			return a;
		}
}
