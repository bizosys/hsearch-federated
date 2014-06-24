import java.util.BitSet;

/**
 * A while back a popular article hit Hacker News. Written by the guys over at
 * Spool, it contained a slick methodology for storing metrics such as user
 * logins per day, song plays by user, etc using using Redis BitSets. How about
 * a basic example. When a user logs in, set a bit in a bitset at the location
 * of that user�s ID number. If you have a bitset allocated for each day, you
 * can tell for any given day how many users logged in by looking at the
 * cardinality of the bitset. Want to see if a particular user logged in on a
 * particular day? Just check the location in the bitset for that user�s ID for
 * the day in question for a 1 value. You can also perform more advanced
 * logging, taking the union of multiple sets, or the disunion, to determine
 * various statistics. The theory behind it is simple and sound. It�s faster
 * than hitting an RDBMS for values that are binary in nature, and the ability
 * to apply basic set theory to your bitsets to analyze your metrics is quite
 * powerful.
 * 
 * @author abinash
 * 
 */
public class BitSetExample {
  public static void main(String args[]) {
	  long loops = 1000000000/5000000;
	  
	  long s = System.currentTimeMillis();
	  for ( int i=0; i<loops; i++) AA();
	  long e = System.currentTimeMillis();
	  
	  System.out.println("Time taken :" + ( e -s ));
    
  }

public static void AA() {
	/**
	   * query1 = Matching IDS 1 Million Documents 1-100 and rest even numbers.
	   *          Make List of all document positions 
	   * query2 = Matching IDS 1 Million Documents 1-100 and rest odd numbers.
	   * 		  Make List of all document positions	
	   * query1 AND query2
	   * query1 OR query2
	   */
	 long s = System.currentTimeMillis();
	 long m = Runtime.getRuntime().freeMemory();

	 BitSet bits1 = new BitSet();
	 for ( int i=0; i<100; i++) bits1.set(i);
	 for ( int i=100; i<5000000; i++) if ( i % 2 == 0 ) bits1.set(i);
	 
	 BitSet bits2 = new BitSet();
	 for ( int i=0; i<100; i++) bits2.set(i);
	 for ( int i=100; i<5000000; i++) if ( i % 2 != 0 ) bits2.set(i);
	 
	/**
	bits1.and(bits2);
	bits2.and(bits1);
	*/

	bits1.and(bits2); 
	bits2.and(bits1);
	 
    long e = System.currentTimeMillis();
    long x = Runtime.getRuntime().freeMemory();
    
    System.out.println(bits1.size() + "/ " + bits2.size() + " in ms :" + ( e -s ) + ", memory : " + (m-x)/1024 );
    
    /**
    if ( bits2.get(0)) System.out.println(0);
    if ( bits2.get(1000000 - 1)) System.out.println(1000000 - 1);

    if ( bits1.get(0)) System.out.println(0);
    if ( bits1.get(1000000 - 1)) System.out.println(1000000 - 1);
    */
    long bitsT = bits1.size();
    for ( int i=0; i<bitsT; i++) {
    	//if ( bits1.get(i)) System.out.print(i);
    }
    //System.out.println("");
    
    long bits2T = bits2.size();
    for ( int i=0; i<bits2T; i++) {
    	//if ( bits2.get(i)) System.out.print(i);
    }    
    //System.out.println("\n---");

    //System.out.println(bits1.toString());
    //System.out.println(bits2.toString());
}
}