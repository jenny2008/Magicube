package cn.ict.cn.galios;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.File;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.io.Writer;
import java.nio.Buffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import cn.ict.cn.galios.Number;

public class FileSplitProcessing {
	
	public static void main(String[] args) throws Exception {
		
		File fin = new File("/tmp/10.txt");
		int n = 100, k = 50, r = 15;
		Matrix<Galios8Num> EMatrix = FileProcessingTest.genEMatrix(k, n);
//		System.out.println(EMatrix.toString());
		
		FileOutputStream[] fos = createDesFiles(fin, n);
		
		// t0 is the start time of read and split source file
		long t0 = System.currentTimeMillis();
		readFile(fin, fos, r, k, EMatrix);		
		for (int i = 0; i < k; i++) {
			fos[i].close();
		}
		//t1 is the finish time of file split.
		
		long t1 = System.currentTimeMillis();
		System.out.println("File spliting time is:" + (t1 -t0));
		
		//reconstruct big file
		int[] A = {1, 3, 5, 7, 9, 11 ,13, 15, 17, 19,
				   21, 23, 25, 27, 29, 31, 33, 35, 37, 39,
				   41, 43, 45, 47, 49, 51, 53, 55, 57, 59,
				   61, 63, 65, 67, 69, 71, 73, 75, 77, 79,
				   81, 83, 85, 87, 89, 91, 93, 95, 97, 99};

	//	int[] A = {1, 3, 4, 5};
		
		Matrix<Galios8Num> Ek = genEk(A, EMatrix);
		Matrix<Galios8Num> VEk = Ek.getVer();
		
		FileInputStream[] fis = getInputFile(fin.getParent(), A);
	
//		FileDescriptor f = fis[0].getFD();
		
//		f.
		
		
		
		FileOutputStream fout = creatRecFile("/tmp/re10.txt");
		
		Matrix<Galios8Num> Fk = null;
		// t2 is the start time of 
		long t2 = System.currentTimeMillis();
	
		do {
			Fk = genFk(A, fis, r);
			
			if (Fk != null) {
				reconFile(Fk, VEk, fout);
			}
			
		} while (Fk != null);
//	*/	
		
		for (int i = 0; i < fis.length; i++) {
			fis[i].close();
		}
		
		fout.close();
		// t3 is the finish time of file reconstruction
		long t3 = System.currentTimeMillis();
		
		System.out.println("File reconstruction time is: " + (t3 - t2));
	}
	
	/* this function read source file by blocks 
	* and transform a rows * cols Matrix block into rows * n block, 
	* then put each col of this result Matrix into a output file.
	* by doing this, a source file can be change into n files.
	*/
	public static void readFile (File fin, FileOutputStream[] fos, int rows, int cols, 
										Matrix<Galios8Num> EMatrix) throws Exception {
				
	//	BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fin)));
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(fin));
		
//		FileInputStream fis = new FileInputStream(fin);
/*
		int bsize = rows * cols;
		int count = (int) (fin.length() / bsize); 
		int c = count;
		while (count != 0) {
			Matrix<Galios8Num> F = writeFileBlock(bis, rows, cols);
			
			if (F != null) {
				Matrix<Galios8Num> nchunks = F.mul(EMatrix);
				writeFiles(nchunks, fos);
			}
			count--;
		}
// deal with the rest file tail which can't be a whole block.
		
		int lsize = (int) (fin.length() - (c * bsize));
		
		if (lsize > 0) {
			int lrows = (lsize + (cols -1)) / cols;
			Matrix<Galios8Num> LF = new Matrix<Galios8Num> (rows, cols, 
					Galios8Num.getOne(), Galios8Num.getZero());
			for (int i = 0; i < lrows; i++) {
				for (int j = 0; j < cols; j++) {
					int in = bis.read();
					if (in != -1) {
						if (in < 0 || in > 255) {
							throw new Exception(
									String.format("elements out of range 0x%x ", in));
						}
						LF.set(i, j, Galios8Num.getNum(in));
					}
				}
			}
			writeFiles(LF.mul(EMatrix), fos);
		}
	*/			
 		Matrix<Galios8Num> F = null;
		do {
			F = writeFileBlock(bis, rows, cols);
			
			if (F != null) {
				Matrix<Galios8Num> nchunks = F.mul(EMatrix);
				writeFiles(nchunks, fos);
			}
						
		} while (F != null);
		
		bis.close();
	}
	
	public static FileOutputStream[] createDesFiles (File fin, int n) throws FileNotFoundException {
		FileOutputStream[] res = new FileOutputStream[n];
		for (int i = 0; i < n; i++) {
			String fn = fin.getParent() + File.separator + "file" + Integer.toString(i);
			File f = new File(fn);
			f.delete();
			res[i] = new FileOutputStream(f);
		}
		
		return res;
	}
	
	public static void writeFiles (Matrix<Galios8Num> nchunks, FileOutputStream[] fos) throws IOException {
				
		for (int j = 0; j < nchunks.getCols(); j++) {
									
			if (fos[j] == null) {
				System.out.println("fout doesn't exist!");
			} else {
				for (int i = 0; i < nchunks.getRows(); i++) {
					fos[j].write(nchunks.get(i, j).toByte());
				}
			}
		}
	}
	
	public static void writeFile (BufferedWriter bw, int j, Matrix<Galios8Num> nchunks) throws IOException {
		for (int i = 0; i < nchunks.getRows(); i++) {
			Galios8Num ch = nchunks.get(i, j);
			bw.write(ch.toInt());
		}
	}
	
	public static Matrix<Galios8Num> writeFileBlock (BufferedInputStream bis,
			int rows, int cols) throws Exception {
		
		Matrix<Galios8Num> mx = new Matrix<Galios8Num> (rows, cols, 
					Galios8Num.getOne(), Galios8Num.getZero());			
			for (int i = 0; i < rows; i++) {
				for (int j = 0; j < cols; j++) {			
					int in = bis.read();		
					if (in != -1) {
						mx.set(i, j, Galios8Num.getNum(in));
					} else {
						if ((i == 0) && (j == 0))
							return null;
					}
				}
			}
			//System.out.println(mx.toString());
		return mx;
	}

	public static void reconFile (Matrix<Galios8Num> Fk, Matrix<Galios8Num> VEk, FileOutputStream recf) throws Exception {
		Matrix<Galios8Num> res = Fk.mul(VEk);
		
		for (int i = 0; i < res.getRows(); i++) {
			for (int j = 0; j < res.getCols(); j++) {
				recf.write(res.get(i, j).toByte());
			}
		}
		
	} 

	public static FileOutputStream creatRecFile (String filename) throws FileNotFoundException {
		File f = new File(filename);
		f.delete();
		
		FileOutputStream recf = new FileOutputStream(f);
		
		return recf;
	}
	
	public static FileInputStream[] getInputFile (String base, int[] A) throws FileNotFoundException {
		FileInputStream[] fis = new FileInputStream[A.length];
		/*
		File f = new File("/tmp");
		String[] fn = f.list(new Test.MyFilter());*/
		//Arrays.sort(fn);

		for (int i = 0; i < A.length; i++) {

			String fname = base + File.separator + "file" + Integer.toString(A[i]);
			fis[i] = new FileInputStream(new File(fname));		
		}
	
		return fis;
	}
	
	public static Matrix<Galios8Num> genFk (int[] A, FileInputStream[] fis, int rows) throws Exception {
		Matrix<Galios8Num> res = new Matrix<Galios8Num> (rows, A.length, 
										Galios8Num.getOne(), Galios8Num.getZero());
		for (int j = 0; j < A.length; j++) {
			for (int r = 0; r < rows; r++) {
				int in = fis[j].read();
				
				if (in != -1) {
					res.set( r, j, Galios8Num.getNum(in));
				} else {

					if ((r == 0) && (j == 0))
						return null;
				}
				
			}
		}
		
		return res;
	}
	
	public static Matrix<Galios8Num> genEk (int[] A, Matrix<Galios8Num> EMatrix) throws Exception {
		Matrix<Galios8Num> res = new Matrix<Galios8Num> (EMatrix.getRows(), A.length, 
				Galios8Num.getOne(), Galios8Num.getZero());
		
		for (int j = 0; j < A.length; j++) {
			for (int i = 0; i < EMatrix.getRows(); i++) {
				res.set(i, j, EMatrix.get(i, A[j]));
			}
		}
		
		return res;		
	}
	
	public static void readFileByChars(String fileName) {
        File file = new File(fileName);
        Reader reader = null;
        try {
            System.out.println("以字符为单位读取文件内容，一次读一个字节：");
            // 一次读一个字符
            reader = new InputStreamReader(new FileInputStream(file));
            int tempchar;
            while ((tempchar = reader.read()) != -1) {
                // 对于windows下，\r\n这两个字符在一起时，表示一个换行。
                // 但如果这两个字符分开显示时，会换两次行。
                // 因此，屏蔽掉\r，或者屏蔽\n。否则，将会多出很多空行。
                if (((char) tempchar) != '\r') {
                    System.out.print((char) tempchar);
                }
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            System.out.println("以字符为单位读取文件内容，一次读多个字节：");
            // 一次读多个字符
            char[] tempchars = new char[30];
            int charread = 0;
            reader = new InputStreamReader(new FileInputStream(fileName));
            // 读入多个字符到字符数组中，charread为一次读取字符数
            while ((charread = reader.read(tempchars)) != -1) {
                // 同样屏蔽掉\r不显示
                if ((charread == tempchars.length)
                        && (tempchars[tempchars.length - 1] != '\r')) {
                    System.out.print(tempchars);
                } else {
                    for (int i = 0; i < charread; i++) {
                        if (tempchars[i] == '\r') {
                            continue;
                        } else {
                            System.out.print(tempchars[i]);
                        }
                    }
                }
            }

        } catch (Exception e1) {
            e1.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
    }
	
	
}
