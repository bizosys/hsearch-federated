/*
* Copyright 2010 Bizosys Technologies Limited
*
* Licensed to the Bizosys Technologies Limited (Bizosys) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The Bizosys licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/


package com.bizosys.hsearch.federate;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.oneline.ferrari.TestAll;

public class FederatedSearchBitSetsTest extends TestCase {
	public static String[] modes = new String[] { "all", "random", "method"};
	public static String mode = modes[1];  
	
	public static void main(String[] args) throws Exception {
		FederatedSearchBitSetsTest t = new FederatedSearchBitSetsTest();
		
		if ( modes[0].equals(mode) ) {
			TestAll.run(new TestCase[]{t});
		} else if  ( modes[1].equals(mode) ) {
	        TestFerrari.testRandom(t);
	        
		} else if  ( modes[2].equals(mode) ) {
			t.setUp();
			t.testAndWithNullMatching();
			t.tearDown();
		}
	}

	@Override
	protected void setUp() throws Exception {
	}
	
	@Override
	protected void tearDown() throws Exception {
	}
	
	public void testAndWithNullMatching() throws Exception {
		FederatedSearch ff = createFederatedSearch(); 
		Map<String, QueryPart> queryDetails = new HashMap<String, QueryPart>();
		
		String query = "(q1 AND q6)";
		BitSetOrSet q1q2Result = ff.execute(query, queryDetails);
		assertEquals(0, q1q2Result.getDocumentSequences().cardinality());
	}	
	
	public void testAndWithManyMatching() throws Exception {
		FederatedSearch ff = createFederatedSearch(); 
		Map<String, QueryPart> queryDetails = new HashMap<String, QueryPart>();
		
		String query = "(q1 AND q2)";
		BitSetOrSet q1q2Result = ff.execute(query, queryDetails);
		
		int size = 0;
		for ( int i=0; i<q1q2Result.getDocumentSequences().size(); i++) {
			if ( ! q1q2Result.getDocumentSequences().get(i)  ) continue;
			size++;
			assertTrue( i >= 75 &&  i < 100 );	
		}
		assertEquals(25, size);
	}

	public void testAndWithOneMatching() throws Exception {
		FederatedSearch ff = createFederatedSearch(); 
		Map<String, QueryPart> queryDetails = new HashMap<String, QueryPart>();
	
		String query = "(q1 AND q3)";
		BitSetOrSet q1q3Result = ff.execute(query, queryDetails);

		int size = 0;
		for ( int i=0; i<q1q3Result.getDocumentSequences().size(); i++) {
			if ( ! q1q3Result.getDocumentSequences().get(i)  ) continue;
			size++;
			assertTrue( i == 0 );	
		}
		assertEquals(1, size);
	}

	public void testAndWithNoMatching() throws Exception {

		FederatedSearch ff = createFederatedSearch(); 
		Map<String, QueryPart> queryDetails = new HashMap<String, QueryPart>();
	
		String query = "(q2 AND q3)";
		BitSetOrSet q2q3Result = ff.execute(query, queryDetails);
		
		int size = 0;
		for ( int i=0; i<q2q3Result.getDocumentSequences().size(); i++) {
			if ( ! q2q3Result.getDocumentSequences().get(i)  ) continue;
			size++;
		}
		assertEquals(0, size);
	}
	
	public void testMultiAnd() throws Exception {
		
		FederatedSearch ff = createFederatedSearch(); 
		Map<String, QueryPart> queryDetails = new HashMap<String, QueryPart>();

		String query = "(q1 AND q1 AND q1 AND q1 AND q1 AND q2 AND q1 AND q1 AND q1 AND q1 AND q1 AND q1 AND q1 AND q1)";
		BitSetOrSet manyResult = ff.execute(query, queryDetails);

		int size = 0;
		for ( int i=0; i<manyResult.getDocumentSequences().size(); i++) {
			if ( ! manyResult.getDocumentSequences().get(i)  ) continue;
			size++;
		}
		assertEquals(25, size);
	}

	public void testOverlappingOR() throws Exception {

		FederatedSearch ff = createFederatedSearch(); 
		Map<String, QueryPart> queryDetails = new HashMap<String, QueryPart>();

		String query = "(q1 OR q2)";
		BitSetOrSet manyResult = ff.execute(query, queryDetails);

		int size = 0;
		for ( int i=0; i<manyResult.getDocumentSequences().size(); i++) {
			if ( ! manyResult.getDocumentSequences().get(i)  ) continue;
			assertTrue(i >= 0 && i<=200 );
			size++;
		}
		assertEquals(200, size);

	}		
	
	public void bracketTestEnd() throws Exception {
		FederatedSearch ff = createFederatedSearch();
		Map<String, QueryPart> queryDetails = new HashMap<String, QueryPart>();
		
		 // q1 = 0 - 100 , q2 = 75 - 200, q3 = 0
		String query = "q1 AND (q2 OR q3)";

		BitSetOrSet manyResult = ff.execute(query, queryDetails);
		int size = 0;
		for ( int i=0; i<manyResult.getDocumentSequences().size(); i++) {
			if ( ! manyResult.getDocumentSequences().get(i)  ) continue;
			assertTrue(i == 0 || ( i>=75 && i<100) );
			size++;
		}
		assertEquals(26, size);		
	}		
	
	public void bracketTestStart() throws Exception {
		FederatedSearch ff = createFederatedSearch();
		Map<String, QueryPart> queryDetails = new HashMap<String, QueryPart>();
		
		 // q1 = 0 - 100 , q2 = 75 - 200, q3 = 0
		String query = "(q2 AND q3) OR q1";

		BitSetOrSet manyResult = ff.execute(query, queryDetails);
		int size = 0;
		for ( int i=0; i<manyResult.getDocumentSequences().size(); i++) {
			if ( ! manyResult.getDocumentSequences().get(i)  ) continue;
			assertTrue( i>=0 && i<100 );
			size++;
		}
		assertEquals(100, size);		
	}			

	public void testNonOverlappingOR() throws Exception {
		
		FederatedSearch ff = createFederatedSearch();
		Map<String, QueryPart> queryDetails = new HashMap<String, QueryPart>();
		
		queryDetails.put("sql:q1", new QueryPart("*|*|m|*|*") );
		queryDetails.put("htable:q4", new QueryPart("*|*|f|[10-20]|*") );

		// q1 = 0 - 100 , q2 = 75 - 200, q3 = 0
		String query = "(sql:q1 OR htable:q4)";

		BitSetOrSet manyResult = ff.execute(query, queryDetails);
		int size = 0;
		for ( int i=0; i<manyResult.getDocumentSequences().size(); i++) {
			if ( ! manyResult.getDocumentSequences().get(i)  ) continue;
			assertTrue( (i >= 0 &&  i < 100) || (i >= 200 &&  i < 300) );		
			size++;
		}
		assertEquals(200, size);
	}			


	public void testPerformance() throws Exception {
		FederatedSearch ff = createFederatedSearch(); 
		Map<String, QueryPart> queryDetails = new HashMap<String, QueryPart>();
		
		queryDetails.put("sql:q5", new QueryPart("*|*|m|*|*") );
		queryDetails.put("htable:q5", new QueryPart("*|*|f|[10-20]|*") );
		
		String query = "(sql:q5 OR htable:q5)";
		long s = System.currentTimeMillis();
		BitSetOrSet manyResult = ff.execute(query, queryDetails);
		long e = System.currentTimeMillis();
		System.out.println(manyResult.getDocumentSequences().size() + " in ms " + (e-s));
		
	}
	
	private FederatedSearch createFederatedSearch() {
		FederatedSearch ff = new FederatedSearch(2) {

			@Override
			public BitSetOrSet populate(
					String type, String queryId, String queryDetail, Map<String, Object> params) {
				
				long s = System.currentTimeMillis();
				System.out.println("Populate:" );
				BitSetOrSet rows = new BitSetOrSet();
				
				if ( null != params) {
					for (Object param : params.values()) {
						System.out.println(param);
					}
				}
				
				BitSet bits = new BitSet();
				if ( queryId.equals("q1")) {
					for ( int i=0; i<100; i++) bits.set(i);
				} else if ( queryId.equals("q2")) {
					for ( int i=75; i<200; i++) bits.set(i);
				} else if ( queryId.equals("q3")) {
					for ( int i=0; i<1; i++) bits.set(i);
				} else if ( queryId.equals("q4")) {
					for ( int i=200; i<300; i++) bits.set(i);
				} else if ( queryId.equals("q5")) {
					for ( int i=1; i<5000000; i++) bits.set(i);
				} 
				
				if ( queryId.equals("q6")) {
					rows.setDocumentSequences(null);
				} else {
					rows.setDocumentSequences(bits);
				}
				
				long e = System.currentTimeMillis();
				System.out.println("Null docment sequences" + rows.getDocumentSequences().size());
				System.out.println("Rows :" + rows.getDocumentSequences().size() + " in ms " + (e-s));
				return rows;
			}
			
		};
		return ff;
	}
		
		
}