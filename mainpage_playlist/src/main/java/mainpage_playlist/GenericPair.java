package mainpage_playlist;

public class GenericPair<A extends Comparable, B  extends Comparable> implements Comparable {
	public A first;
	public B second;
	
	public GenericPair(A first, B second)
	{
		this.first = first;
		this.second = second;
	}
	
	@Override
	public int compareTo(Object arg0) {
		GenericPair b = (GenericPair) arg0;
		return first.compareTo(b.first);
	}
	@Override
	public String toString()
	{
		return String.valueOf(first) +"\t" + String.valueOf(second);
	}
	
	public static void main(String[]args)
	{
		 GenericPair<Integer,Float > p1 = new GenericPair<Integer,Float >(1,2.0f);
		 GenericPair<Integer,Float > p2 = new GenericPair<Integer,Float >(2,2.0f);
		 System.out.println(p2.compareTo(p1));
	}
}
