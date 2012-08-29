package cn.ict.cn.galios;

import java.nio.Buffer;

public class Galios8Num implements Number<Galios8Num> {
	public static class GaliosException extends Exception {
		private static final long serialVersionUID = 1L;
	}
	public static class GaliosNumOutOfBoundException extends GaliosException {
		private static final long serialVersionUID = 1L;
	}
	public static class GaliosNumDividedByZeroException extends GaliosException {
		private static final long serialVersionUID = 1L;
	}
	
	private static int[] alog = new int[256];
	private static int[] log = new int[256];
	private static Galios8Num[] allGalios8Num = new Galios8Num[256];
	
	private Galios8Num() throws Exception {
		throw new Exception("create new Galios8 object without parameters");
	}
	
	static {
		int i, j;
		alog[0] = 1;
		for(i=1; i<256; i++) {
			j = (alog[i-1] << 1) ^ alog[i-1];
			if(((j & 0x100) != 0)) 
				j ^= 0x11B;
			alog[i] = j;
		}
		
		log[0] = log[1] = 0;
		for(i=1; i < 255; i++) {
			log[alog[i]] = i;
		}
		
		for (i = 0; i < 256; i++) {
			try {
				allGalios8Num[i] = new Galios8Num(i);
			} catch (GaliosNumOutOfBoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static Galios8Num getZero() {
		return allGalios8Num[0];
	}
	
	public static Galios8Num getOne() {
		return allGalios8Num[1];
	}
	
	private int val;
	
	private Galios8Num(int x) throws GaliosNumOutOfBoundException {
		if ((x < 0) || (x > 255))
			throw new GaliosNumOutOfBoundException();
		val = x;
	}
	
	public int getV() {
		return val;
	}
	
	public static Galios8Num getNum(int x) throws GaliosNumOutOfBoundException {
		return allGalios8Num[x % 256];
	}
	public static Galios8Num getNum(byte x) throws GaliosNumOutOfBoundException {
		return getNum((int)x);
	}	
	public static Galios8Num getNum(char x) throws GaliosNumOutOfBoundException {
		return getNum((int)x);
	}
	public static Galios8Num getNum(Galios8Num x) {
		return x;
	}
	
	public static Galios8Num[] getAllGalios8Nums() {
		return allGalios8Num.clone();
	}
	

	@Override
	public boolean isOne() {
		return this.val == 1;
	}

	@Override
	public boolean isZero() {
		return this.val == 0;
	}
	
	public Galios8Num add(Galios8Num x) {
		int A = this.val;
		int B = x.val;
		try {
			return getNum(A ^ B);
		} catch (GaliosNumOutOfBoundException e) {
			e.printStackTrace();
			return getZero();
		}
	}

	public Galios8Num mul(Galios8Num x) {
		if ((this.isZero()) || (x.isZero())) {
			return getZero();
		}
		int A = this.val;
		int B = x.val;
		
		try {
			return getNum(alog[(log[A] + log[B]) % 255]);
		} catch (GaliosNumOutOfBoundException e) {
			e.printStackTrace();
			return getOne();
		}
	}
	
	public Galios8Num getMulInv() throws GaliosNumDividedByZeroException {
		if (this.isZero())
			throw new GaliosNumDividedByZeroException();
		try {
			return getNum(alog[255 - log[this.val]]);
		} catch (GaliosNumOutOfBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return getOne();
		}
	}
	
	public Galios8Num getAddInv() {
		return this;
	}
	
	public Galios8Num div(Galios8Num x) throws GaliosNumDividedByZeroException {
		/* fast path */

		int A = this.val;
		int B = x.val;		

		try {
			if (A == 0) {
				return getZero();
			} else if((log[A]-log[B] < 0)) {
				return getNum(alog[(log[A] - log[B] + 255) % 255]);
			} else {
				return getNum(alog[(log[A] - log[B]) % 255]);
			}
		} catch (GaliosNumOutOfBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return getOne();
		}
		/*
		 
		if(A == 0)
			return 0;
		else if(B == 0) {
			System.err.println("XX divide by zero exception");
			return -1;			
		} else if((log[A]-log[B] < 0)) {
	//		System.out.println(String.format("div %d %d", A, B));
			return alog[(log[A] - log[B] + 255) % 255];
		} else
			return alog[(log[A]-log[B]) % 255];
		 * 
		 */
		
	//	return this.mul(x.getMulInv());
	}
	
	public Galios8Num minus(Galios8Num x) {
		return add(x);
		//return this.add(x.addInvert());
	}
	
	@Override
	public Galios8Num pow(int x) throws Exception {
		if (x < 0) {
			throw new Exception(this.toString() + "^(0)");
		}
		return getNum(alog[log[this.val] * x % 255]);
	}
	
	@Override
	public Galios8Num getZeroInterface() throws Exception {
		// TODO Auto-generated method stub
		return getZero();
	}

	@Override
	public Galios8Num getOneInterface() throws Exception {
		// TODO Auto-generated method stub
		return getOne();
	}

	public String toString() {
		return String.format("0x%02x", val); 
	}
	
	@Override	
	public byte toByte() {
		return (byte)(val);
	}
	
	@Override
	public int toInt() {
		return val;
	}
		
	public static void main(String[] argv) throws Exception {
		Galios8Num zero = Galios8Num.getZero();
		Galios8Num one = Galios8Num.getOne();
		Galios8Num three = Galios8Num.getNum(3);
		Galios8Num x = Galios8Num.getNum(0x77);
		
		System.out.println("1 + 1 = " + one.add(one));
		System.out.println("1 + 0x77 = " + one.add(x));
		
		System.out.println("1 * 1 = " + one.mul(one));
		System.out.println("1 * 0x77 = " + one.mul(x));
		
		
		System.out.println("0 * 1 = " + zero.mul(one));
		System.out.println("0 * 0x77 = " + zero.mul(x));
		
		System.out.println("0x77 * 0x77 = " + x.mul(x));
		
		System.out.println("0x77 * (0x77)^(-1) = " + x.mul(x.getMulInv()));
		System.out.println("0x77 / 0x77 = " + x.div(x));
		
		for (int i = 0; i < 256; i++) {
			System.out.println(String.format("0x03^%d=%s", i, three.pow(i).toString()));
		}
	}

	@Override
	public Galios8Num[] createArray(int n) {
		Galios8Num[] r = new Galios8Num[n];
		for (int i = 0; i < r.length; i++) {
			r[i] = this;
		}
		return r;
		
	}
	
	@Override
	public Galios8Num[][] createMatrix(int rows, int cols) {
		Galios8Num[][] r = new Galios8Num[rows][cols];
		for (int c = 0; c < rows; c++) {
			for (int l = 0; l < cols; l++) {
				r[c][l] = this;
			}
		}
		return r;	
	}
}
