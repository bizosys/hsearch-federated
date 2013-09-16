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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class FederatedSearch {
	
	private static boolean DEBUG_MODE = FederatedSearchLog.l.isDebugEnabled();
	private static boolean INFO_MODE = FederatedSearchLog.l.isInfoEnabled();
	
	private static ExecutorService es = null;
	
	public FederatedSearch() {
		this (-1);
	}
	
	public static ExecutorService getExecutorService() {
		if ( null != es ) return es;
		init(-1);
		return es;
	}
	
	private static void init(final int fixedThreads) {
		if ( null == es) {
			synchronized ("FederatedFacade") {
				if ( null == es) {
					if ( fixedThreads > 0 ) {
						es = Executors.newFixedThreadPool(fixedThreads);
					} else {
						es = Executors.newFixedThreadPool(
							Runtime.getRuntime().availableProcessors() * 4);
					}
				}
			}
		}
	}

	
	public FederatedSearch(final int fixedThreads) {
		init(fixedThreads);
	}

	public final BitSetOrSet execute(final String query, final Map<String, QueryPart> queryArgs) 
		throws FederatedSearchException, IOException, InterruptedException {
		
		if ( DEBUG_MODE) FederatedSearchLog.l.debug("FederatedFacade.execute Main - ENTER ");

		try {
			HQuery hquery = new HQueryParser().parse(query);
	
			List<HTerm> terms = new ArrayList<HTerm>();
			new HQuery().toTerms(hquery, terms);

			int termsT = (null == terms) ? 0 : terms.size();
			if ( termsT < 0 ) termsT = 1;
			List<FederatedSource> sources = new ArrayList<FederatedSource>(termsT);
			
			for (HTerm aTerm : terms) {
				FederatedSource fs = new FederatedSource(this);
				fs.setTerm(aTerm);
				
				String name = aTerm.text;
				if ( null != aTerm.type ) {
					if ( aTerm.type.length() > 0 ) name =  aTerm.type + ":" + name;
				}
				if ( DEBUG_MODE) FederatedSearchLog.l.debug("Term :" + name);
				
				fs.setQueryDetails(queryArgs.get(name));
				sources.add(fs);
			}
	
			if ( DEBUG_MODE) FederatedSearchLog.l.debug("FederatedFacade.execute Query Setting - COMPLETED ");
			
			int sourcesT = sources.size();
			
			if (sourcesT == 1) {
				sources.get(0).execute();
			} else {
			
				if ( INFO_MODE )FederatedSearchLog.l.info(
					Thread.currentThread().getName() + " > Parallel Execution of Sub Queries .. " + sourcesT);
				es.invokeAll(sources);
			}
			
			if ( DEBUG_MODE) FederatedSearchLog.l.debug("FederatedFacade.execute Query Populate- COMPLETED ");
			
			BitSetOrSet finalResult = new BitSetOrSet();
			new HQueryCombiner().combine(hquery, finalResult);
			return finalResult;
			
		} finally {
			if ( DEBUG_MODE) FederatedSearchLog.l.debug("FederatedFacade.execute Main - EXIT ");
		}
		
	}
	
	private HQuery hquery = null;
	private HQueryCombiner combiner = null;
	List<HTerm> terms = null;
	List<FederatedSource> sources = new ArrayList<FederatedSource>();
	BitSetOrSet finalResult = new BitSetOrSet();
	
	public final void initialize(final String query) 
			throws FederatedSearchException, IOException, InterruptedException {
			
			if ( DEBUG_MODE) FederatedSearchLog.l.debug("FederatedFacade.initialize - ENTER > " + query);

			this.hquery = new HQueryParser().parse(query);
			this.combiner = new HQueryCombiner();
			this.terms = new ArrayList<HTerm>();

			int termsT = (null == terms) ? 0 : terms.size();
			if ( termsT <= 0 ) termsT = 1;
			new HQuery().toTerms(hquery, terms);
			if ( DEBUG_MODE) FederatedSearchLog.l.debug( "Terms:" + terms.toString());
			
			if ( DEBUG_MODE) FederatedSearchLog.l.debug("FederatedFacade.execute initialize - EXIT ");
		}

	
	public final BitSetOrSet execute(final Map<String, QueryPart> queryArgs) 
			throws FederatedSearchException, IOException, InterruptedException {
			
			if ( DEBUG_MODE) FederatedSearchLog.l.debug("FederatedFacade.execute Main - ENTER ");

			try {
				sources.clear();
				for (HTerm aTerm : terms) {
					FederatedSource fs = new FederatedSource(this);
					fs.setTerm(aTerm);
					
					String name = aTerm.text;
					if ( null != aTerm.type ) {
						if ( aTerm.type.length() > 0 ) name =  aTerm.type + ":" + name;
					}
					if ( DEBUG_MODE) FederatedSearchLog.l.debug("Term :" + name);
					
					fs.setQueryDetails(queryArgs.get(name));
					sources.add(fs);
				}
		
				if ( DEBUG_MODE) FederatedSearchLog.l.debug("FederatedFacade.execute Query Setting - COMPLETED ");
				
				int sourcesT = sources.size();
				
				if (sourcesT == 1) {
					sources.get(0).execute();
				} else {
					if ( INFO_MODE )FederatedSearchLog.l.info(
						Thread.currentThread().getName() + " > Parallel Execution of Sub Queries .. " + sourcesT);
					es.invokeAll(sources);
				}
				
				if ( DEBUG_MODE) FederatedSearchLog.l.debug("FederatedSearch.execute Query Populate- COMPLETED ");
				
				combiner.reset();
				finalResult.reset();
				combiner.combine(hquery, finalResult);
				return finalResult;
				
			} finally {
				if ( DEBUG_MODE) FederatedSearchLog.l.debug("FederatedSearch.execute Main - " + finalResult.size() + " EXIT ");
			}
			
		}	
	
	
	/**
	 *  queryId 	:	q1, q2
	 *  type    	:	sql, htable
	 *  aStmtOrValue:	*|*|m|*|* ,  *|*|f|[10-20]|*
	 *  sql:q1		:	new QueryPart("*|*|m|*|*")
	 *  htable:q4	:	new QueryPart("*|*|f|[10-20]|*")
	 *  (sql:q1 OR htable:q4)";
	 */
	public abstract BitSetOrSet populate(
		String type, String queryId, String aStmtOrValue, Map<String, Object> stmtParams) throws IOException;
	
}
