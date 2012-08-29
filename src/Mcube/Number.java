package Mcube;

import java.nio.Buffer;

public interface Number<T> {
	public T add(T x);
	public T mul(T x);
	public T minus(T x);
	public T div(T x) throws Exception;
	
	public T getMulInv() throws Exception;
	public T getAddInv();
	
	public T getZeroInterface() throws Exception;
	public T getOneInterface() throws Exception;
	
	public T pow(int x) throws Exception;
	
	public boolean isOne() throws Exception;
	public boolean isZero() throws Exception;
	
	public byte toByte();
	public int toInt();
	public String toString();
	
	public T[] createArray(int n);
	public T[][] createMatrix(int rows, int cols);
		
}

