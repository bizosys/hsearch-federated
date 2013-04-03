package com.bizosys.hsearch.federate;

/**
 * Federated Source Output
 * @author Abinasha Karana, Bizosys
 */
public class HResult {
	
	private BitSetOrSet foundIds = null;
	
	public BitSetOrSet getRowIds() {
		if ( null == this.foundIds) {
			FederatedSearchLog.l.warn(Thread.currentThread().getName() + " > HResult get has null values");
		}
		return this.foundIds ;
	}

	public void setRowIds(BitSetOrSet foundIds) {
		this.foundIds = foundIds;

		if ( null == this.foundIds) {
			FederatedSearchLog.l.warn(Thread.currentThread().getName() + " > HResult set has null values");
		}
	}
	
	@Override
	public String toString() {
		if ( null == foundIds)	return "null";
		return foundIds.toString();
	}

}
