package com.bizosys.hsearch.federate;


/**
 * Combines output from all the sources with appropriate sequencing
 * @author Abinasha Karana, Bizosys
 */
public final class HQueryCombiner {

	private static boolean DEBUG_MODE = FederatedSearchLog.l.isDebugEnabled();
	//private static boolean INFO_MODE = FederatedSearchLog.l.isInfoEnabled();
	
	public final  BitSetOrSet combine(final HQuery query, final BitSetOrSet destination, boolean keepProcessingTrace) throws FederatedSearchException  {
		
		try {
			/**
			 * Imaging we are iterating through directories
			 */
			for (HQuery subQuery : query.subQueries) {
				/**
				 * Just execute the subquery.
				 */
				BitSetOrSet subQueryOutput = new BitSetOrSet();
				combine(subQuery, subQueryOutput, keepProcessingTrace); 
				if ( DEBUG_MODE ) {
					int subQuerySize = ( null == subQueryOutput) ? 0 : subQueryOutput.size();
					FederatedSearchLog.l.debug("Launching a Sub Query: EXIT " + 
							subQuery.toString() + "\t" + subQuerySize);
				}
				
				if ( subQuery.isMust ) {
					
					if ( DEBUG_MODE ) {
						int destSize = ( null == destination) ? 0 : destination.size();
						int subQuerySize = ( null == subQueryOutput) ? 0 : subQueryOutput.size();
						FederatedSearchLog.l.debug("Sub Query Must: " + destination.isVirgin +
							"\tDestination:\t" + destSize + "\tOutput\t" + subQuerySize );
					}
					if ( destination.isVirgin ) {
						destination.or(subQueryOutput);
						
						/**
						if (keepProcessingTrace)
							destination.orQueryWithFoundIdsTemp.putAll(subQueryOutput.orQueryWithFoundIds);
						*/
						
					} else {
						destination.and(subQueryOutput);

						/**
						if (keepProcessingTrace) destination.orQueryWithFoundIdsTemp.clear();
						*/

					}
					
				} else if ( subQuery.isShould ) {
					if ( DEBUG_MODE ) {
						int destSize = ( null == destination) ? 0 : destination.size();
						int subQuerySize = ( null == subQueryOutput) ? 0 : subQueryOutput.size();
						FederatedSearchLog.l.debug("Sub Query Should: " + destination.isVirgin +
							"\tDestination:\t" + destSize + "\tOutput\t" + subQuerySize );
					}

					/**
					if (keepProcessingTrace) {
						destination.orQueryWithFoundIds.putAll(destination.orQueryWithFoundIdsTemp);
						destination.orQueryWithFoundIds.putAll(subQueryOutput.orQueryWithFoundIds);
						destination.orQueryWithFoundIdsTemp.clear();
					}
					*/
					destination.or(subQueryOutput);

				} else {
					if ( DEBUG_MODE ) {
						int destSize = ( null == destination) ? 0 : destination.size();
						int subQuerySize = ( null == subQueryOutput) ? 0 : subQueryOutput.size();
						FederatedSearchLog.l.debug("Sub Query Not: " + destination.isVirgin + "\tDestination:\t" + 
							destSize + "\tOutput\t" + subQuerySize );
					}
					/**
					if (keepProcessingTrace) {
						destination.orQueryWithFoundIdsTemp.clear();
					}
					*/
					destination.not(subQueryOutput);
				}
				
				/**
				 * In all cases, take the OR queries
				 */
				if ( keepProcessingTrace && (subQuery.isMust || subQuery.isShould ) ) {
					destination.orQueryWithFoundIds.putAll(subQueryOutput.orQueryWithFoundIds); 
					destination.orQueryWithFoundIds.putAll(subQueryOutput.orQueryWithFoundIdsTemp); 
				}
				
				subQueryOutput.clear();
				destination.isVirgin = false;
				if ( DEBUG_MODE ) {
					int destSize = ( null == destination) ? 0 : destination.size();
					FederatedSearchLog.l.debug("Sub Query Updated Destination: " + destSize  );
				}
			}

			/**
			 * Imaging we are iterating files of this directories
			 */
			
			//AND Terms
			for (HTerm term : query.terms) {
				
				if ( ! term.isMust ) continue;

				HResult source = term.getResult();
				
				if ( null == source) {
					destination.isVirgin = false;
					destination.clear();
					return destination;
				}
				
				if ( destination.isVirgin ) {
				
					if ( DEBUG_MODE ) FederatedSearchLog.l.debug("First Must :" + term.text + "\tsource:" + source);

					if ( keepProcessingTrace ) virginAndTrace(destination, term, source);
					
					destination.or(source.getRowIds());
					destination.isVirgin = false;

				} else {
					destination.and(source.getRowIds());
					
					if ( keepProcessingTrace ) virginAndTraceRollback(destination);
					
					if ( DEBUG_MODE ) FederatedSearchLog.l.debug("Subsequent Must :" + term.text + "\n" + 
							"source:" + source + "\tdestination\t" + destination);
				}
			}
			
			//OR Terms
			for (HTerm term : query.terms) {
				if ( ! term.isShould ) continue;

				HResult source = term.getResult();
				if ( null == source) continue;
				
				if ( destination.isVirgin ) destination.isVirgin = false;
				
				if ( DEBUG_MODE ) FederatedSearchLog.l.debug(
					Thread.currentThread().getName() + " > OR :" + term.text);

				if ( keepProcessingTrace ) orTrace(destination, term, source);

				destination.or(source.getRowIds());				
			}
			
			//NOT Terms
			for (HTerm term : query.terms) {
			
				if ( term.isShould ) continue;
				if ( term.isMust) continue;

				if ( destination.isVirgin ) {
					throw new FederatedSearchException("Only must not query not allowed");
				} else {
					HResult source = term.getResult();
					if ( null == source) continue;

					destination.not( source.getRowIds());
					if ( DEBUG_MODE ) FederatedSearchLog.l.debug("Not :" + term.text + ":");
					
				}
			}
			
			return destination;
			
		} catch (Exception e) {
			e.printStackTrace(System.err);
			FederatedSearchLog.l.error(query.toString(), e);
			throw new FederatedSearchException(e);
		}
	}

	/**
	 * This is the Second term and OR is confirmed.
	 */
	private void orTrace(final BitSetOrSet destination, HTerm term, HResult source)
			throws FederatedSearchException {
		BitSetOrSet trace = new BitSetOrSet();
		trace.or(source.getRowIds());
		destination.orQueryWithFoundIds.put(term.text, trace);
		destination.orQueryWithFoundIds.putAll(destination.orQueryWithFoundIdsTemp);
	}

	/**
	 * This is the Second term and AND is confirmed. Remove from temp
	 */
	private void virginAndTraceRollback(final BitSetOrSet destination ) {
		//destination.orQueryWithFoundIdsTemp.clear();
	}

	/**
	 * This is the first term, So treated as OR. Put in temp.
	 */
	private void virginAndTrace(final BitSetOrSet destination, HTerm term, HResult source) throws FederatedSearchException {

		BitSetOrSet trace = new BitSetOrSet();
		trace.or(source.getRowIds());
		destination.orQueryWithFoundIdsTemp.put(term.text, trace);
		
		HResult res = term.getResult();
		if ( null != res ) {
			BitSetOrSet innerQueries = res.getRowIds();
			if ( null != innerQueries) 
				destination.orQueryWithFoundIdsTemp.putAll(innerQueries.orQueryWithFoundIds);
		}
	}
	
	public final void reset() {
	}
}
