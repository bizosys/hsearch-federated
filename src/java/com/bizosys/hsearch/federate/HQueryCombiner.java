package com.bizosys.hsearch.federate;


/**
 * Combines output from all the sources with appropriate sequencing
 * @author Abinasha Karana, Bizosys
 */
public final class HQueryCombiner {

	private static boolean DEBUG_MODE = FederatedSearchLog.l.isDebugEnabled();
	//private static boolean INFO_MODE = FederatedSearchLog.l.isInfoEnabled();
	
	public final  BitSetOrSet combine(final HQuery query, final BitSetOrSet destination) throws FederatedSearchException  {
		
		try {
			/**
			 * Imaging we are iterating through directories
			 */
			for (HQuery subQuery : query.subQueries) {
				/**
				 * Just execute the subquery.
				 */
				BitSetOrSet subQueryOutput = new BitSetOrSet();
				combine(subQuery, subQueryOutput); 
				if ( DEBUG_MODE ) FederatedSearchLog.l.debug("Launching a Sub Query: EXIT " + subQuery.toString() + "\t" + subQueryOutput.toString());
				
				if ( subQuery.isMust ) {
					if ( DEBUG_MODE ) FederatedSearchLog.l.debug("Sub Query Must: " + destination.isVirgin + "\tDestination:\t" + destination.toString() + "\tOutput\t" + subQueryOutput.toString() );
					if ( destination.isVirgin ) {
						destination.or(subQueryOutput);
					} else {
						destination.and(subQueryOutput);
					}
				} else if ( subQuery.isShould ) {
					if ( DEBUG_MODE ) FederatedSearchLog.l.debug("Sub Query Should: " + destination.isVirgin + "\tDestination:\t" + destination.toString() + "\tOutput\t" + subQueryOutput.toString() );
					destination.or(subQueryOutput);
				} else {
					if ( DEBUG_MODE ) FederatedSearchLog.l.debug("Sub Query Not: " + destination.isVirgin + "\tDestination:\t" + destination.toString() + "\tOutput\t" + subQueryOutput.toString() );
					destination.not(subQueryOutput);
				}
				
				subQueryOutput.clear();
				destination.isVirgin = false;
				if ( DEBUG_MODE ) FederatedSearchLog.l.debug("Sub Query Updated Destination: " + destination.toString()  );
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
					destination.or(source.getRowIds());
					destination.isVirgin = false;

				} else {
					destination.and(source.getRowIds());
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
	
	public final void reset() {
	}
}
