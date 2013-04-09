package com.bizosys.hsearch.federate;


/**
 * Combines output from all the sources with appropriate sequencing
 * @author Abinasha Karana, Bizosys
 */
public final class HQueryCombiner {

	private static boolean DEBUG_MODE = FederatedSearchLog.l.isDebugEnabled();
	//private static boolean INFO_MODE = FederatedSearchLog.l.isInfoEnabled();
	
	boolean isFirst = true;
	
	public final  BitSetOrSet combine(final HQuery query, final BitSetOrSet destination) throws FederatedSearchException  {
		
		try {
			
			for (HQuery subQuery : query.subQueries) {
				if ( DEBUG_MODE ) FederatedSearchLog.l.debug("Launching a Sub Query");
				combine(subQuery, destination);
			}
			
			//AND Terms
			for (HTerm term : query.terms) {
				
				if ( ! term.isMust ) continue;

				HResult source = term.getResult();
				if ( null == source) continue;
				
				if ( isFirst ) {
				
					if ( DEBUG_MODE ) FederatedSearchLog.l.debug("First Must :" + term.text);

					destination.or(source.getRowIds());
					isFirst = false;

				} else {
				
					destination.and(source.getRowIds());

					if ( DEBUG_MODE ) FederatedSearchLog.l.debug(
						Thread.currentThread().getName() + "Subsequnt Must :" + term.text);
				}
			}
			
			//OR Terms
			for (HTerm term : query.terms) {
				
				if ( ! term.isShould ) continue;

				HResult source = term.getResult();
				if ( null == source) continue;
				
				if ( isFirst ) isFirst = false;
				
				if ( DEBUG_MODE ) FederatedSearchLog.l.debug(
					Thread.currentThread().getName() + " > OR :" + term.text);

				destination.or(source.getRowIds());				
			}
			
			//NOT Terms
			for (HTerm term : query.terms) {
			
				if ( term.isShould ) continue;
				if ( term.isMust) continue;

				if ( isFirst ) {
				
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
		this.isFirst = true;
	}
	

}
