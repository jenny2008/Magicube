package cn.ict.cn.galios;

public class Rational implements Number<Rational> {

	
	private float val;
	
	Rational(float x) {
		val = x;
	}
	
	@Override
	public Rational add(Rational x) {
		return new Rational(val + ((Rational)x).val);
	}

	@Override
	public Rational mul(Rational x) {
		return new Rational(val * ((Rational)x).val);
	}

	@Override
	public Rational minus(Rational x) {
		return new Rational(val - ((Rational)x).val);
	}

	@Override
	public Rational div(Rational x) throws Exception {
		return new Rational(val / ((Rational)x).val);
	}

	@Override
	public Rational getMulInv() throws Exception {
		return new Rational(1.0f / val);
	}

	@Override
	public Rational getAddInv() {
		return new Rational(-val);
	}

	@Override
	public Rational getZeroInterface() {
		return new Rational(0);
	}

	@Override
	public Rational getOneInterface() {
		return new Rational(1);
	}

	@Override
	public Rational pow(int x) {
		float v = 1;
		for (int i = 0; i < x; i++) {
			v *= this.val;
		}
		return new Rational(v);
	}

	@Override
	public boolean isOne() {
		return (val == 1.0f);
	}

	@Override
	public boolean isZero() throws Exception {
		// TODO Auto-generated method stub
		return (val == 0.0f);
	}

	@Override
	public byte toByte() {
		return ((byte)(val));
	}

	@Override
	public int toInt() {
		return ((int)(val));
	}
	
	public String toString() {
		return String.format("%f", val);
	}

	@Override
	public Rational[] createArray(int n) {
		Rational[] r = new Rational[n];
		for (int i = 0; i < r.length; i++) {
			r[i] = new Rational(this.val);
		}
		return r;
	}

	@Override
	public Rational[][] createMatrix(int rows, int cols) {
		Rational[][] r = new Rational[rows][cols];
		for (int c = 0; c < rows; c++) {
			for (int l = 0; l < cols; l++) {
				r[c][l] = new Rational(this.val);
			}
		}
		
		return r;	
	}
	
	public static void swap(Galios8Num[] a, int i, int n) {
		Galios8Num temp = a[i];	    
		a[i] = a[n];
		a[n] = temp;
	}
	
	public static int random(int a, int b) {
		int c = 0;
		java.util.Random r = new java.util.Random();
		// get a random int number which is in a~b
		c = Math.abs(r.nextInt())%(b-a)+a;
		
		
		return c;
	}
	
	//shuffle algorithm set the array elment into a random arrange
	public static Galios8Num[] shuffle(Galios8Num[] A) {
	// for 1:n swap(a[i], a[random(i, n)])			
		for(int i=0;i<A.length;i++) {
			swap(A, i, random(i, A.length));			
		}
		
		return A;
	}

}
