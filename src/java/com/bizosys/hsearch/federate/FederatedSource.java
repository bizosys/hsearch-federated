package com.bizosys.hsearch.federate;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * For each source it runs inside a thread.
 * @author Abinasha Karana, Bizosys
 */
public final class FederatedSource implements Callable<Integer> {

	private static boolean DEBUG_MODE = FederatedSearchLog.l.isDebugEnabled();
	//private static boolean INFO_MODE = FederatedSearchLog.l.isInfoEnabled();
	
	HTerm term = null;
	QueryPart queryMappings = null;
	FederatedSearch searcher = null;
	
	public FederatedSource(final FederatedSearch searcher) {
		this.searcher = searcher;
	}
	
	@Override
	public final Integer call() throws Exception {
		this.execute();
		return 0;
	}
	
	protected final void execute() throws FederatedSearchException, IOException {
		if ( DEBUG_MODE) FederatedSearchLog.l.debug(
			Thread.currentThread().getName() + " > FederatedFacade.execute : ENTER");
		
		Map<String, Object> params = ( null == this.queryMappings) ? new HashMap<String, Object>() : queryMappings.getParams();
		String q = ( null == queryMappings) ? "" : queryMappings.aStmtOrValue;
		
		HResult result = new HResult();
		BitSetOrSet matchingIds = searcher.populate(term.type, term.text, q, params);
		
		if ( DEBUG_MODE) FederatedSearchLog.l.debug( Thread.currentThread().getName() +
				" > FederatedFacade.execute populate completed with output : null = " + ( null == matchingIds) );
		
		result.setRowIds(matchingIds);

		if ( null == result.getRowIds()) {
			FederatedSearchLog.l.debug("WARNING : result.foundIds null");
		} else {
			if ( DEBUG_MODE) FederatedSearchLog.l.debug("DEBUG : result.foundIds " + result.getRowIds().toString() );
		}
		
		term.setResult(result);
		
		if ( DEBUG_MODE) FederatedSearchLog.l.debug(
			Thread.currentThread().getName() + " > " + 
			result.getRowIds().orQueryWithFoundIds.toString() + 
			"  > FederatedFacade.execute : EXIT");
		
	}
	
	public final void setTerm(HTerm term) {
		this.term = term;
	}
	
	public final void setQueryDetails(final QueryPart queryMappings) {
		this.queryMappings = queryMappings;
	}
	
	public final HTerm getTerm() {
		return this.term;
	}
	
	public final  QueryPart getQueryDetails() {
		return this.queryMappings;
	}
}
