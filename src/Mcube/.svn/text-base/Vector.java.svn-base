package Mcube;

import Mcube.Number;

public class Vector<T extends Number<T>> {
	
	public static class VectorSizeNotEqualException extends Exception {
		private static final long serialVersionUID = 1L;
	}
	
	public static class VectorSizeUnMatchException extends Exception {
		private static final long serialVersionUID = 1L;
	}
	
	private T[] innerM;
	private T one;
	private T zero;
	
	private int size;
	
	Vector (int size, T one, T zero) {
		innerM = zero.createArray(size);
		this.size = size;
		this.one = one;
		this.zero = zero;
	}
	
	public int getSize() {
		return size;
	}
	
	public void set(int i, T n) {
		innerM[i] = n;
	}
	
	public T get(int i) {
		return innerM[i];
	}

	// to judge if two vectors are equal 
	public boolean isEqual(Vector a, Vector b) {
		if (a.size != b.size)
			return false;
		else {
			for (int i = 0; i < a.size; i++) {
				if ((a.get(i) != b.get(i))) {
					return false;
				} else
					continue;
			}
			
			return true;
		}
	}
	
	// to get a vecotr's negative vector
	public Vector<T> getNeg() {
		Vector<T> vx = new Vector<T>(size, one, zero);
		for (int i = 0; i < vx.size; i++) {
			//a.set(i, a.get(i).getAddInv());
			vx.innerM[i] = this.innerM[i].getAddInv();
		}
		return vx;
	}
	
	// this function can return the union of two vectors
	public Vector<T> add(Vector<T> a) throws Exception {
		Vector<T> vx = new Vector<T>(size, one, zero);

		if (a.size != this.size) {
			throw new VectorSizeNotEqualException();
		} else {
			for (int i = 0; i < a.size; i++) {
				vx.set(i, this.get(i).add(a.get(i)));
			}
		}
		
		return vx;
	}
	
	// define the vector's multiplication by number
	public Vector<T> numMul(T k) {
		Vector<T> vx = new Vector<T>(size, one, zero);
		
		for (int i = 0; i < this.size; i++) {
			vx.set(i, this.get(i).mul(k));
		}
		
		return vx;
	}
	
	// define the operation of vector multiply Matrix
	public Vector<T> mulMatrix(Matrix<T> a) throws Exception {
		
		if (this.size != a.getRows()) {
			throw new VectorSizeUnMatchException();
		}
			Vector<T> vx = new Vector<T>(size, one, zero);
		
		for (int j = 0; j < a.getCols(); j++) {
			T tmp = zero;
			for (int i = 0; i < this.size; i++) {
				tmp.add(this.get(i).mul(a.get(i, j)));
			}	
			
			vx.set(j, tmp);
		}
			
		return vx;
	}
		
}

