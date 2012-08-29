package cn.ict.cn.galios;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.lang.reflect.Array;
import java.util.Arrays;

public class Test {
	public static  void main(String[] args) throws FileNotFoundException {
		File f = new File("/tmp");
		//System.out.println(f.getPath());
		String[] fs = f.list(new MyFilter());
		for (int i = 0; i < fs.length; i++) {
			System.out.println(fs[i]);
		}
		
		Arrays.sort(fs);
		System.out.println("after sort:");
		for (int i = 0; i < fs.length; i++) {
			System.out.println(fs[i]);
		}
		
		int[] A = {1, 3, 5, 7};
		
		System.out.println("fis[i]");
//		FileInputStream[] fis = FileSplitProcessing.getInputFile(A);
/*
		for (int i = 0; i < fis.length; i++) {
			System.out.println(fis[i]);
		}
*/		
		
	} 
	
	public static class MyFilter implements FilenameFilter {
		@Override
		public boolean accept(File dir, String name) {
			// TODO Auto-generated method stub
			if (name.startsWith("file"))
				return true;
			return false;
		}
		
	}
}
