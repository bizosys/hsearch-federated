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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.oneline.ferrari.TestAll;

public class FederatedSearchTest extends TestCase {
	public static String[] modes = new String[] { "all", "random", "method"};
	public static String mode = modes[2];  
	
	public static void main(String[] args) throws Exception {
		FederatedSearchTest t = new FederatedSearchTest();
		
		if ( modes[0].equals(mode) ) {
			TestAll.run(new TestCase[]{t});
		} else if  ( modes[1].equals(mode) ) {
	        TestFerrari.testRandom(t);
	        
		} else if  ( modes[2].equals(mode) ) {
			t.setUp();
			t.testAndIdTypeString();
			t.tearDown();
		}
	}

	@Override
	protected void setUp() throws Exception {
	}
	
	@Override
	protected void tearDown() throws Exception {
	}
	
	public void testAnd() throws Exception {
		FederatedFacade<Long, Integer> ff = createFederatedSearch(); 

		Map<String, QueryPart> queryDetails = new HashMap<String, QueryPart>();
		
		String query = "(q1 AND q2)";
		List<FederatedFacade<Long, Integer>.IRowId> q1q2Result = ff.execute(query, queryDetails);
		assertEquals(25, q1q2Result.size());
		for (FederatedFacade<Long, Integer>.IRowId iRowId : q1q2Result) {
			assertTrue( (iRowId.getDocId() >= 75 &&  iRowId.getDocId() < 100) );		
		}
		
		query = "(q1 AND q3)";
		List<FederatedFacade<Long, Integer>.IRowId> q1q3Result = ff.execute(query, queryDetails);
		assertEquals(1, q1q3Result.size());
		for (FederatedFacade<Long, Integer>.IRowId iRowId : q1q3Result) {
			assertTrue( (iRowId.getDocId() == 0) );		
		}
		
		query = "(q2 AND q3)";
		List<FederatedFacade<Long, Integer>.IRowId> q2q3Result = ff.execute(query, queryDetails);
		assertEquals(0, q2q3Result.size());

		query = "(q1 AND q1 AND q1 AND q1 AND q1 AND q1 AND q1 AND q1 AND q1 AND q1 AND q1 AND q1 AND q1 AND q1)";
		List<FederatedFacade<Long, Integer>.IRowId> manyResult = ff.execute(query, queryDetails);
		assertEquals(100, manyResult.size());
		for (FederatedFacade<Long, Integer>.IRowId iRowId : q1q2Result) {
			assertTrue( (iRowId.getDocId() >= 0 &&  iRowId.getDocId() < 100) );		
		}		
	}

	public void testAndIdTypeString() throws Exception {
		FederatedFacade<Long, String> ff = createFederatedSearchString(); 

		Map<String, QueryPart> queryDetails = new HashMap<String, QueryPart>();
		
		String query = "(q1 AND q2)";
		List<FederatedFacade<Long, String>.IRowId> q1q2Result = ff.execute(query, queryDetails);
		assertEquals(25, q1q2Result.size());
		for (FederatedFacade<Long, String>.IRowId iRowId : q1q2Result) {
			assertTrue( ( new Integer( iRowId.getDocId() ) >= 75 &&  new Integer( iRowId.getDocId() ) < 100) );		
		}
		
		query = "(q1 AND q3)";
		List<FederatedFacade<Long, String>.IRowId> q1q3Result = ff.execute(query, queryDetails);
		assertEquals(1, q1q3Result.size());
		for (FederatedFacade<Long, String>.IRowId iRowId : q1q3Result) {
			Integer i = new Integer( iRowId.getDocId() );
			assertTrue( i.intValue() == 0 );		
		}
		
		query = "(q2 AND q3)";
		List<FederatedFacade<Long, String>.IRowId> q2q3Result = ff.execute(query, queryDetails);
		assertEquals(0, q2q3Result.size());

		query = "(q1 AND q1 AND q1 AND q1 AND q1 AND q1 AND q1 AND q1 AND q1 AND q1 AND q1 AND q1 AND q1 AND q1)";
		List<FederatedFacade<Long, String>.IRowId> manyResult = ff.execute(query, queryDetails);
		assertEquals(100, manyResult.size());
		for (FederatedFacade<Long, String>.IRowId iRowId : q1q2Result) {
			Integer i = new Integer( iRowId.getDocId() );
			assertTrue( i.intValue()  >= 0 &&  i.intValue() < 100 );		
		}		
	}

	public void testMultiThreads() throws Exception {
		FederatedFacade<Long, Integer> ff = createFederatedSearch();
		Map<String, QueryPart> queryDetails = new HashMap<String, QueryPart>();
		
		String query = "(q1 AND q1 AND q1 AND q1 AND q1 AND q1 AND q1 AND q1 AND q1 AND q1 AND q1 AND q1 AND q1 AND q1)";
		List<FederatedFacade<Long, Integer>.IRowId> manyResult = ff.execute(query, queryDetails);
		assertEquals(100, manyResult.size());
		for (FederatedFacade<Long, Integer>.IRowId iRowId : manyResult) {
			assertTrue( (iRowId.getDocId() >= 0 &&  iRowId.getDocId() < 100) );		
		}		
	}	
	
	public void testOverlappingOR() throws Exception {
		FederatedFacade<Long, Integer> ff = createFederatedSearch();
		Map<String, QueryPart> queryDetails = new HashMap<String, QueryPart>();
		
		String query = "(q1 OR q2)";
		List<FederatedFacade<Long, Integer>.IRowId> manyResult = ff.execute(query, queryDetails);
		assertEquals(200, manyResult.size());
		for (FederatedFacade<Long, Integer>.IRowId iRowId : manyResult) {
			assertTrue( (iRowId.getDocId() >= 0 &&  iRowId.getDocId() < 200) );		
		}		
	}		
	
	public void testNonOverlappingOR() throws Exception {
		FederatedFacade<Long, Integer> ff = createFederatedSearch();
		Map<String, QueryPart> queryDetails = new HashMap<String, QueryPart>();
		
		queryDetails.put("sql:q1", new QueryPart("*|*|m|*|*") );
		queryDetails.put("htable:q4", new QueryPart("*|*|f|[10-20]|*") );
		
		String query = "(sql:q1 OR htable:q4)";
		List<FederatedFacade<Long, Integer>.IRowId> manyResult = ff.execute(query, queryDetails);
		assertEquals(200, manyResult.size());
		for (FederatedFacade<Long, Integer>.IRowId iRowId : manyResult) {
			assertTrue( 
					(iRowId.getDocId() >= 0 &&  iRowId.getDocId() < 100) || 
					(iRowId.getDocId() >= -200 &&  iRowId.getDocId() < -100) 
					);		
		}		
	}			
	
	
	private FederatedFacade<Long, Integer> createFederatedSearch() {
		FederatedFacade<Long, Integer> ff = new FederatedFacade<Long, Integer>(0, 100, 10) {

			@Override
			public List<FederatedFacade<Long, Integer>.IRowId> populate(
					String type, String queryId, String queryDetail, Map<String, Object> params) {

				List<FederatedFacade<Long, Integer>.IRowId> rows = new ArrayList<FederatedFacade<Long, Integer>.IRowId>();
				
				System.out.println("type:" + type);
				System.out.println("queryId:" + queryId);
				System.out.println("queryDetail:" + queryDetail);
				if ( null != params) {
					for (Object param : params.values()) {
						System.out.println(param);
					}
				}
				
				if ( queryId.equals("q1")) {
					for ( int i=0; i<100; i++) rows.add(new KeyBucket(0L, i));
				} else if ( queryId.equals("q2")) {
					for ( int i=75; i<200; i++) rows.add(new KeyBucket(0L, i));
				} else if ( queryId.equals("q3")) {
					for ( int i=-11; i<1; i++) rows.add(new KeyBucket(0L, i));
				} else if ( queryId.equals("q4")) {
					for ( int i=-200; i<-100; i++) rows.add(new KeyBucket(0L, i));
				}
				return rows;
			}
			
		};
		return ff;
	}
	
	private FederatedFacade<Long, String> createFederatedSearchString() {
		FederatedFacade<Long, String> ff = new FederatedFacade<Long, String>("", 100, 10) {

			@Override
			public List<FederatedFacade<Long, String>.IRowId> populate(
					String type, String queryId, String queryDetail, Map<String, Object> params) {

				List<FederatedFacade<Long, String>.IRowId> rows = new ArrayList<FederatedFacade<Long, String>.IRowId>();
				
				System.out.println("type:" + type);
				System.out.println("queryId:" + queryId);
				System.out.println("queryDetail:" + queryDetail);
				if ( null != params) {
					for (Object param : params.values()) {
						System.out.println(param);
					}
				}
				
				if ( queryId.equals("q1")) {
					for ( int i=0; i<100; i++) rows.add(new KeyBucket(0L, new Integer(i).toString()));
				} else if ( queryId.equals("q2")) {
					for ( int i=75; i<200; i++) rows.add(new KeyBucket(0L, new Integer(i).toString()) );
				} else if ( queryId.equals("q3")) {
					for ( int i=-11; i<1; i++) rows.add(new KeyBucket(0L, new Integer(i).toString()) );
				} else if ( queryId.equals("q4")) {
					for ( int i=-200; i<-100; i++) rows.add(new KeyBucket(0L, new Integer(i).toString()) );
				}
				return rows;
			}
			
		};
		return ff;
	}
		
}