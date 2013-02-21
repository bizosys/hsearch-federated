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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.thirdparty.guava.common.collect.HashMultimap;
import org.apache.hadoop.thirdparty.guava.common.collect.Multimap;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.Version;

public abstract class FederatedFacade<K,V> {
	
	ConcurrentRowJoiner JOINER = new ConcurrentRowJoiner();
	public ObjectFactory objectFactory = new ObjectFactory();
	static Map<String, Query> cachedQueries = new HashMap<String, Query>();
	ExecutorService es = null;
	
	boolean DEBUG_MODE = false;
	
	
	public FederatedFacade(V val, int initialObjects, int fixedThreads) {
		if ( fixedThreads < 2) fixedThreads = 2;
		es = Executors.newFixedThreadPool(fixedThreads);
		
		for ( int i=0; i<initialObjects; i++) {
			objectFactory.putPrimaryKeyRowId(new KeyPrimary(val));
		}
	}
	
	/**
	 * //////////////////////////////////////////////////////////////////////////////////////////////////////////
	 * Generic Row. It can take partition as well as document id.
	 * @author Abinasha Karana, Bizosys
	 * //////////////////////////////////////////////////////////////////////////////////////////////////////////
	 */
	public abstract class IRowId {
		public abstract K  getPartition();
		public abstract V getDocId();
		public abstract void setDocId(V key);
		public abstract void setPartitionId(K id);
	}

	/**
	 * //////////////////////////////////////////////////////////////////////////////////////////////////////////
	 * This is a typical non partitioned key.
	 * @author Abinasha Karana, Bizosys
	 * //////////////////////////////////////////////////////////////////////////////////////////////////////////
	 */
	public class KeyPrimary extends IRowId {

		V key = null;
		K id = null;
		public KeyPrimary(V key) {
			this.key = key;
		}
		
		@Override
		public final K getPartition() {
			return null;
		}

		@Override
		public final V getDocId() {
			return this.key;
		}
		
		@Override
		public void setDocId(V key) {
			this.key = key;
		}
		
		@Override
		public void setPartitionId(K id) {
			this.id = id;
		}

	}
	

	/**
	 * //////////////////////////////////////////////////////////////////////////////////////////////////////////
	 * Data is bucketed. Each bucket contains multiple Ids
	 * @author Abinasha Karana, Bizosys
	 * //////////////////////////////////////////////////////////////////////////////////////////////////////////
	 */
	public class KeyBucket extends IRowId {
		
		/**
		 * Partition Id 
		 */
		public K partitionId;
		
		/**
		 * Document Serial Id 
		 */
		public V serialId;
		
		public KeyBucket ( K partitionId, V docId ) {
			this.partitionId = partitionId;
			this.serialId = docId;
		}

		@Override
		public K getPartition() {
			return this.partitionId;
		}

		@Override
		public V getDocId() {
			return this.serialId;
		}
		
		@Override
		public void setDocId(V key) {
			this.serialId = key;
		}
		
		@Override
		public void setPartitionId(K id) {
			this.partitionId = id;
		}
		
	}
	
	/**
	 * //////////////////////////////////////////////////////////////////////////////////////////////////////////
	 * It Joins a row Ids 
	 * @author Abinasha Karana, Bizosys
	 * //////////////////////////////////////////////////////////////////////////////////////////////////////////
	 */
	public class ConcurrentRowJoiner {
		public void and (List<IRowId> input1, List<IRowId> input2) {

			if ( null == input1 || null == input2) {
				return;
			}
			
			if ( 0 == input1.size()|| 0 == input2.size()) {
				input1.clear();
				input2.clear();
				return;
			}
			
			List<IRowId> set1 = null;
			List<IRowId> set2 = null;
			if ( input1.size() > input2.size()) {
				set1 = input2;
				set2 = input1;
			} else {
				set1 = input1;
				set2 = input2;
			}
			
			/**
			 * Step 1 - Create a Set of Buckets.
			 */
			Set<K> set1Buckets = new HashSet<K>();
			
			for (IRowId id1 : set1) {
				set1Buckets.add(id1.getPartition());
			}

			
			/**
			 * Step 2 - Find intersected buckets
			 */
			Set<K> intersectedBuckets = new HashSet<K>();
			for (IRowId id2 : set2) {
				if ( set1Buckets.contains(id2.getPartition())) 
					intersectedBuckets.add(id2.getPartition());
			}
			set1Buckets.clear();
			
			/**
			 * Step 3 - Cleanse unnecessary elements set 1
			 */
			keepMatchings(set1, intersectedBuckets);
			
			/**
			 * Step 4 - Cleanse unnecessary elements set 2
			 */
			keepMatchings(set2, intersectedBuckets);
			
			/**
			 * Step 5 - Extract Set 1 from Set 2
			 */
			subtract(set1, set2);

			/**
			 * Step 6 - Extract Set 2 from Set 1
			 */
			subtract(set2, set1);
		}
		
		public void not (List<IRowId> input1, List<IRowId> input2) {

			Multimap<K,V> mmm = HashMultimap.create();
			for (IRowId aRecord : input2) {
				mmm.put(aRecord.getPartition(), aRecord.getDocId());
			}
			
			/**
			 * Discard Now
			 */
			List<IRowId> notInRecords = new ArrayList<IRowId>();
			for( IRowId record : input1) {
				if ( mmm.get(record.getPartition()).contains(record.getDocId()) ) continue;
				notInRecords.add(record);
			}
			input1.clear();
			input1.addAll(notInRecords);
			notInRecords.clear();
			mmm.clear();

		}

		
		public void or (List<IRowId> input1, List<IRowId> input2) {
			
			Multimap<K,V> mmm = HashMultimap.create();
			
			if ( null != input1) {
				for (IRowId recordId : input1) {
					mmm.put(recordId.getPartition(), recordId.getDocId());
				}
			}

			List<IRowId> delta = new ArrayList<IRowId>();
			if ( null != input2) {
				for (IRowId recordId : input2) {
					if ( ! mmm.containsKey(recordId.getPartition()) ) {
						delta.add(recordId);
						continue;
					}
					
					if ( mmm.get(recordId.getPartition()).contains(recordId.getDocId())) continue;
					delta.add(recordId);
				}
			} else {
				if ( DEBUG_MODE )System.out.println("input1 is null");
			}
			mmm.clear();
			input1.addAll(delta);
			delta.clear();
		}	

		private void subtract(List<IRowId> set1, List<IRowId> set2) {
			/**
			 * Set 1 is smaller than Set2
			 */
			Multimap<K,V> mmm = HashMultimap.create();
			for (IRowId aRecord : set1) {
				mmm.put(aRecord.getPartition(), aRecord.getDocId());
			}
			
			/**
			 * Merge Now
			 */
			List<IRowId> matchingRecords = new ArrayList<IRowId>();
			for( IRowId record : set2) {
				if ( ! mmm.get(record.getPartition()).contains(record.getDocId()) ) continue;
					matchingRecords.add(record);
			}
			set2.clear();
			set2.addAll(matchingRecords);
			matchingRecords.clear();
			mmm.clear();
		}

		
		private void keepMatchings(List<IRowId> records, Set<K> intersectedBuckets) {
			int setT = records.size();
			
			int removed = 0;
			IRowId aRecord = null;
			for ( int i=0; i<setT; i++) {
				aRecord = records.get(i);
				if (intersectedBuckets.contains(aRecord.getPartition()) ) continue;
				removed++;
			}
			
			if ( removed > 5000  ) {
				List<IRowId> cleansedLists = new ArrayList<IRowId>(records.size()  - removed);
				for ( int i=0; i<setT; i++) {
					aRecord = records.get(i);
					if ( ! intersectedBuckets.contains(aRecord.getPartition()) ) continue;
					cleansedLists.add(aRecord);
				}
				records.clear();
				records.addAll(cleansedLists);
				cleansedLists.clear();
			} else {
				for ( int i=0; i<setT; i++) {
					aRecord = records.get(i);
					if (intersectedBuckets.contains(aRecord.getPartition()) ) continue;
					records.remove(i);
					i--;
					setT--;
				}				
			}
		}	
	}	
	
	/**
	 * //////////////////////////////////////////////////////////////////////////////////////////////////////////
	 * Lucene Query Population
	 * @author Abinasha Karana, Bizosys
	 * //////////////////////////////////////////////////////////////////////////////////////////////////////////
	 */
	public class HQuery {
		boolean isShould = false;
		boolean isMust = false;
		float boost = 1.0f;
		
		public List<HQuery> subQueries = new ArrayList<HQuery>();
		public List<HTerm> terms = new ArrayList<HTerm>();
		
		public String toString(String level) {
			StringBuilder sb = new StringBuilder();
			sb.append(level).append("**********").append(":Must-");
			sb.append(isMust).append(":Should-").append( isShould).append(":Fuzzy-");
			sb.append(":Boost-").append( boost);;
			for (HQuery query : subQueries) {
				sb.append(query.toString(level + "\t"));
			}
			for (HTerm term : terms) {
				sb.append(level).append(term.type).append(":").append( term.text ).append(":Must-");
				sb.append(term.isMust).append(":Should-").append( term.isShould).append(":Fuzzy-");
				sb.append(term.isFuzzy).append(":").append( term.boost);
			}
			sb.append(level).append("**********");
			return sb.toString();
		}
		
		public void toTerms(HQuery query, List<HTerm> terms) {
			for (HQuery subQuery : query.subQueries) {
				toTerms(subQuery, terms);
			}
			terms.addAll(query.terms);
		}
	}

	/**
	 * //////////////////////////////////////////////////////////////////////////////////////////////////////////
	 * Federated Search Term
	 * @author Abinasha Karana, Bizosys
	 * //////////////////////////////////////////////////////////////////////////////////////////////////////////
	 */
	public class HTerm {
		public boolean isShould = false;
		public boolean isMust = false;
		public float boost = 1;
		public boolean isFuzzy = false;
	
		public String type = null;
		public String text = null;
		public String minRange = null;
		public String maxRange = null;

		HResult result = null;
		public HResult getResult() {
			return result;
		}
		
		public void setResult(HResult result) {
			if ( DEBUG_MODE )System.out.println(Thread.currentThread().getName() + " > setResult " + result);
			this.result = result;
		}

	}	

	
	/**
	 * //////////////////////////////////////////////////////////////////////////////////////////////////////////
	 * Federated Source Output
	 * @author Abinasha Karana, Bizosys
	 * //////////////////////////////////////////////////////////////////////////////////////////////////////////
	 */
	public class HResult {
		
		List<IRowId> foundIds = null;
		
		public List<IRowId> getRowIds() {
			return this.foundIds;
		}

		public void setRowIds(List<IRowId> foundIds) {
			if ( DEBUG_MODE )System.out.println(Thread.currentThread().getName() + " > setRowIds");
			this.foundIds = foundIds;
		}

	}
	
	
	/**
	 * //////////////////////////////////////////////////////////////////////////////////////////////////////////
	 * Combines output from all the sources with appropriate sequencing
	 * @author Abinasha Karana, Bizosys
	 * //////////////////////////////////////////////////////////////////////////////////////////////////////////
	 */
	public class HQueryCombiner {

		public List<IRowId> combine(HQuery query, List<IRowId> finalResult) throws Exception  {
			
			boolean isFirst = true;
			for (HQuery subQuery : query.subQueries) {
				if ( DEBUG_MODE )System.out.println("Launching a Sub Query");
				List<IRowId> subQResult = new ArrayList<IRowId>();
				combine(subQuery, subQResult);
				if ( subQuery.isShould) {
					if ( DEBUG_MODE )System.out.println("OR Joiner of sub query result" +  "> H " + finalResult.hashCode() + ".." + subQResult.hashCode());
					JOINER.or(finalResult, subQResult);
				} else if ( subQuery.isMust) {
					if ( DEBUG_MODE )System.out.println("AND Joiner of sub query result" +  "> H " + finalResult.hashCode() + ".." + subQResult.hashCode());
					JOINER.and(finalResult, subQResult);
				} else {
					if ( DEBUG_MODE )System.out.println("NOT Joiner of sub query result" +  "> H " + finalResult.hashCode() + ".." + subQResult.hashCode());
					JOINER.not(finalResult, subQResult);
				}
				subQResult.clear();
			}
			
			//Find must terms and add them
			for (HTerm term : query.terms) {
				if ( term.isShould ) continue;
				if ( ! term.isMust ) continue;

				if ( isFirst ) {
					finalResult.addAll(term.getResult().getRowIds());
					if ( DEBUG_MODE )System.out.println("First Must :" + term.text + ":" + finalResult.size() + "> H " + finalResult.hashCode() );
					isFirst = false;
				} else {
					JOINER.and(finalResult, term.getResult().getRowIds());
					if ( DEBUG_MODE )System.out.println("Subsequnt Must :" + term.text + ":" + finalResult.size() +  "> H " + finalResult.hashCode() );;
				}
			}
			
			//OR Terms
			for (HTerm term : query.terms) {
				if ( ! term.isShould ) continue;

				if ( isFirst ) {
					finalResult.addAll(term.getResult().getRowIds());
					isFirst = false;
					if ( DEBUG_MODE )System.out.println("First OR :" + term.text + ":" + finalResult.size()  + "> H " + finalResult.hashCode() );
				} else {
					if ( DEBUG_MODE )System.out.println("Subsequent OR :" + term.text + ":" + finalResult.size() + "> H " + finalResult.hashCode());
					JOINER.or(finalResult, term.getResult().getRowIds());
				}
			}
			
			for (HTerm term : query.terms) {
				if ( term.isShould ) continue;
				if ( term.isMust) continue;

				if ( isFirst ) {
					throw new RuntimeException("Only must not query not allowed");
				} else {
					JOINER.not(finalResult, term.getResult().getRowIds());
					if ( DEBUG_MODE )System.out.println("Not :" + term.text + ":" + finalResult.size());
				}
			}
			
			return finalResult;
		}

	}

	
	/**
	 * //////////////////////////////////////////////////////////////////////////////////////////////////////////
	 * Use Lucene Parser to parse the Query
	 * @author Abinasha Karana, Bizosys
	 * //////////////////////////////////////////////////////////////////////////////////////////////////////////
	 */
	public class HQueryParser {

		public HQuery parse(String query) throws Exception {
			WhitespaceAnalyzer analyzer = new WhitespaceAnalyzer(Version.LUCENE_35);

			//TODO: Come up with a logic of finding most freq
			Query qp = null;
			if ( cachedQueries.containsKey(query)) {
				qp = cachedQueries.get(query);
			} else {
				qp = new QueryParser(Version.LUCENE_35,"", analyzer).parse(query);
				cachedQueries.put(query, qp);
			}
			HQuery hQuery = new HQuery();
			parseComposites(qp, hQuery);
			return hQuery;
			
		}
		
		private void parseComposites(Query lQuery, HQuery hQuery) {

			if(lQuery instanceof TermQuery)
			{
				populateTerm(hQuery, false, true, lQuery);
				return;
			}
			
			for (BooleanClause clause : ((BooleanQuery)lQuery).clauses()) {
				
				Query subQueryL = clause.getQuery();

				if ( subQueryL instanceof BooleanQuery ) {

					HQuery subQueryH = new HQuery();
					subQueryH.isShould = clause.getOccur().compareTo(Occur.SHOULD) == 0; 
					subQueryH.isMust = clause.getOccur().compareTo(Occur.MUST) == 0;

					hQuery.subQueries.add(subQueryH);
					parseComposites(subQueryL, subQueryH);
				
				} else {
					boolean isShould = clause.getOccur().compareTo(Occur.SHOULD) == 0; 
					boolean isMust = clause.getOccur().compareTo(Occur.MUST) == 0;
					populateTerm(hQuery, isShould, isMust, subQueryL);
				}
			}
		}

		private void populateTerm(HQuery hQuery, boolean isShould, boolean isMust, Query subQueryL) {
			HTerm hTerm = new HTerm();
			hTerm.isShould = isShould; 
			hTerm.isMust = isMust;
			hTerm.boost = subQueryL.getBoost();
			hQuery.terms.add(hTerm);

			if ( subQueryL instanceof TermQuery ) {
				TermQuery lTerm = (TermQuery)subQueryL;
				hTerm.type = lTerm.getTerm().field();
				hTerm.text = lTerm.getTerm().text();

			} else if ( subQueryL instanceof FuzzyQuery ) {
				FuzzyQuery lTerm = (FuzzyQuery) subQueryL;
				hTerm.isFuzzy = true;
				hTerm.type = lTerm.getTerm().field();
				hTerm.text = lTerm.getTerm().text();
			
			} else if ( subQueryL instanceof TermRangeQuery) {
				TermRangeQuery lTerm = (TermRangeQuery) subQueryL;
				hTerm.isFuzzy = false;
				hTerm.type = lTerm.getField();
				hTerm.minRange =  lTerm.getLowerTerm();
				hTerm.maxRange = lTerm.getUpperTerm();

			} else {
				if ( DEBUG_MODE )System.out.println("Not Implemented Query :" + subQueryL.getClass().toString());;
				hTerm.type = "Not Impemented";
				hTerm.text = "Not Impemented";
			}
		}
	}
	
	/**
	 * //////////////////////////////////////////////////////////////////////////////////////////////////////////
	 * Executes in parallel using multiple threads.
	 * @author Abinasha Karana, Bizosys
	 * //////////////////////////////////////////////////////////////////////////////////////////////////////////
	 */
	public class FederatedExecutor implements Callable<Integer> {

		FederatedSource source = null; 

		public FederatedExecutor(FederatedSource aSource) {
			this.source = aSource;
		}
		
		@Override
		public Integer call() throws Exception {
			try {
				source.execute();
				return 0;
			} catch (Exception ex) {
				throw new Exception(ex);
			}
		}
	}

	public List<IRowId> execute(String query, Map<String, QueryPart> queryArgs) throws Exception {
		HQuery hquery = new HQueryParser().parse(query);

		List<FederatedSource> sources = new ArrayList<FederatedSource>();
		List<HTerm> terms = new ArrayList<HTerm>();
		
		new HQuery().toTerms(hquery, terms);
		
		for (HTerm aTerm : terms) {
			FederatedSource fs = new FederatedSource();
			fs.setTerm(aTerm);
			
			String name = aTerm.text;
			if ( null != aTerm.type ) {
				if ( aTerm.type.length() > 0 ) name =  aTerm.type + ":" + name;
			}
			fs.setQueryDetails(queryArgs.get(name));
			sources.add(fs);
		}

		int sourcesT = sources.size();
		if (sourcesT == 1) {
			sources.get(0).execute();
		} else {
			List<FederatedExecutor> tasks = new ArrayList<FederatedExecutor>();
			
			for (FederatedSource iFederatedSource : sources) {
				tasks.add(new FederatedExecutor(iFederatedSource));
			}
			if ( DEBUG_MODE )System.out.println(Thread.currentThread().getName() + " > Invoking..");

			es.invokeAll(tasks);
			
			if ( DEBUG_MODE )System.out.println(Thread.currentThread().getName() + " > Invoking.. Done");
		}
		
		
		List<IRowId> finalResult = new ArrayList<IRowId>();
		new HQueryCombiner().combine(hquery, finalResult);
		
		return finalResult;
	}
	
	
	/**
	 * //////////////////////////////////////////////////////////////////////////////////////////////////////////
	 * For each source it runs inside a thread.
	 * @author Abinasha Karana, Bizosys
	 * //////////////////////////////////////////////////////////////////////////////////////////////////////////
	 */
	public class FederatedSource {
		
		HTerm term = null;
		QueryPart queryMappings = null;
		
		public FederatedSource() {
		}
		
		protected void execute() throws Exception {
			if ( DEBUG_MODE) System.out.println(Thread.currentThread().getName() + " > FederatedFacade.execute");
			
			Map<String, Object> params = ( null == this.queryMappings) ? new HashMap<String, Object>() : queryMappings.getParams();
			String q = ( null == queryMappings) ? "" : queryMappings.aStmtOrValue;
			
			HResult result = new HResult();
			List<IRowId> matchingIds = populate(term.type, term.text, q, params);
			if ( DEBUG_MODE) System.out.println(Thread.currentThread().getName() + " > FederatedFacade.execute populate");
			
			result.setRowIds(matchingIds);
			if ( DEBUG_MODE) System.out.println(Thread.currentThread().getName() + " > FederatedFacade.execute Matched result.setRowIds");

			if ( null == result.foundIds) if ( DEBUG_MODE) System.out.println("WARNING : result.foundIds null");
			this.term.setResult(result);
			if ( DEBUG_MODE) System.out.println(Thread.currentThread().getName() + " > FederatedFacade.execute Matched term.setResult");

		}
		
		public void setTerm(HTerm term) {
			this.term = term;
		}
		
		public void setQueryDetails(QueryPart queryMappings) {
			this.queryMappings = queryMappings;
		}
		
		public HTerm getTerm() {
			return this.term;
		}
		
		public QueryPart getQueryDetails() {
			return this.queryMappings;
		}
		
	}
	
	public class ObjectFactory {
		
		private int MINIMUM_CACHE = 10;
		private int MAXIMUM_CACHE = 500000;
		
		Stack<IRowId> pkRowIds = new Stack<IRowId>();

		public  void putPrimaryKeyRowId(IRowId aRowId) {
			if ( null == aRowId) return;
			if (pkRowIds.size() > MAXIMUM_CACHE ) return;
			pkRowIds.push(aRowId);
		}	
		
		public  void putprimaryKeyRowId(List<IRowId> usedRows) {
			if ( null == usedRows) return;
			if (pkRowIds.size() > MAXIMUM_CACHE ) return;
			
			for (IRowId keyPrimary : usedRows) {
				pkRowIds.push(keyPrimary);
			}
		}			
		
		public  IRowId getPrimaryKeyRowId(V key) {
			IRowId aRowId = null;
			if (pkRowIds.size() > MINIMUM_CACHE ) aRowId = pkRowIds.pop();
			if ( null != aRowId ) {
				aRowId.setDocId(key);
				return aRowId;
			}
			return new KeyPrimary(key);
		}
		

		public String getStatus() {
			StringBuilder sb = new StringBuilder(476);
			sb.append("<o>");
			sb.append("pkRowIds:").append(pkRowIds.size()).append('|');
			sb.append("</o>");
			return sb.toString();
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
	public abstract List<IRowId> populate(String type, String queryId, String aStmtOrValue, Map<String, Object> stmtParams) throws IOException;
	
}
