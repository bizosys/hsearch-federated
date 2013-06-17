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
			for (HQuery subQuery : query.subQueries) {
				if ( DEBUG_MODE ) FederatedSearchLog.l.debug("Launching a Sub Query");
				BitSetOrSet output = combine(subQuery, new BitSetOrSet());
				if ( subQuery.isMust ) {
					if ( destination.isVirgin) destination.or(output);
					else destination.and(output);
				} else if ( subQuery.isShould ) {
					destination.or(output);
				} else {
					destination.not(output);
				}
				output.clear();
				destination.isVirgin = false;
			}
			
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
				
					if ( DEBUG_MODE ) FederatedSearchLog.l.debug("First Must :" + term.text);
					destination.or(source.getRowIds());
					destination.isVirgin = false;

				} else {
					destination.and(source.getRowIds());
					if ( DEBUG_MODE ) FederatedSearchLog.l.debug("Subsequnt Must :" + term.text + "\n" + 
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
