package com.bizosys.hsearch.federate;

import java.util.concurrent.Callable;

/**
 * Executes in parallel using multiple threads.
 * @author Abinasha Karana, Bizosys
 */
public final class FederatedExecutor implements Callable<Integer> {

	FederatedSource source = null; 

	public FederatedExecutor(final FederatedSource aSource) {
		this.source = aSource;
	}
	
	@Override
	public final Integer call() throws Exception {
		try {
			source.execute();
			return 0;
		} catch (Exception ex) {
			throw new Exception(ex);
		}
	}
}
