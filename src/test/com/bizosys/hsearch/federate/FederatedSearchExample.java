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
import java.util.Random;

import org.apache.hadoop.thirdparty.guava.common.collect.HashMultimap;
import org.apache.hadoop.thirdparty.guava.common.collect.Multimap;
import com.bizosys.hsearch.federate.FederatedFacade;

public class FederatedSearchExample {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		FederatedFacade<String, Long> ff = new FederatedFacade<String, Long>(0L, 100, 2) {

			@Override
			public List<com.bizosys.hsearch.federate.FederatedFacade<String, Long>.IRowId> populate(
					String type, String queryId, String queryDetail, List<String> params) {

				
				
				int howMany = new Random().nextInt(30);
				int startRow = new Random().nextInt(100);
				int endRow = new Random().nextInt(100);
				if ( 0 <= howMany) howMany = 2;
				
				if ( endRow < startRow ) endRow = startRow + 1;
				System.out.println("howMany:" + howMany + ", startR:" + startRow + " , endRow:" + endRow);
				
				List<com.bizosys.hsearch.federate.FederatedFacade<String, Long>.IRowId> rows = 
						new ArrayList<com.bizosys.hsearch.federate.FederatedFacade<String, Long>.IRowId>();
				
				for ( long i=0; i<howMany; i++) {
					for ( long j=startRow; j<endRow; j++) {
						System.out.println(i + ":" + j);
						rows.add(new KeyBucket(new Long(i).toString(), j));
					}
				}
				
				return rows;
			}
			
		}; 

		Map<String, QueryArgs> queryDetails = new HashMap<String, QueryArgs>();
		
		queryDetails.put("structured:q1", new QueryArgs("select id from x where city=?", "bangalore") );
		queryDetails.put("unstructured:q2", new QueryArgs("ABINASH") );
		queryDetails.put("unstructured:q3", new QueryArgs("HADOOP") );
		queryDetails.put("structured:q4", new QueryArgs("select id from y where class=? and section = ?", "IV", "5") );
		String query = "structured:q1 OR unstructured:q2 OR ( unstructured:q3 OR  structured:q4)";
		List<FederatedFacade<String, Long>.IRowId> finalResult = ff.execute(query, queryDetails);
		
		Multimap<String, Long> mmm = HashMultimap.create();
		for (FederatedFacade<String, Long>.IRowId aRecord : finalResult) {
			mmm.put(aRecord.getPartition(), aRecord.getDocId());
		}
		System.out.println(mmm.toString());
	}

}
