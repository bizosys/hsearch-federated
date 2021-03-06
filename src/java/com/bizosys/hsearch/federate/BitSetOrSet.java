package com.bizosys.hsearch.federate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class BitSetOrSet {

	public boolean isVirgin = true;
	private static boolean DEBUG_MODE = FederatedSearchLog.l.isDebugEnabled();
	private static boolean INFO_MODE = FederatedSearchLog.l.isInfoEnabled();

	private BitSetWrapper bitsets = null;
	@SuppressWarnings("rawtypes")
	private Set sets = null;
	
	/**
	 * This contains all parts which have been added to create this.
	 */
	public Map<String, BitSetOrSet> orQueryWithFoundIds = new HashMap<String, BitSetOrSet>(); 
	public Map<String, BitSetOrSet> orQueryWithFoundIdsTemp = new HashMap<String, BitSetOrSet>(); 
	
	public final int size() {
		
		if ( null != sets) return sets.size();

		if ( null != bitsets) {
			return bitsets.cardinality();
		}
		
		return 0;
	}
	
	public final boolean isEmpty() {
		if ( null != bitsets) return bitsets.isEmpty();
		if ( null != sets) return ( sets.size() == 0 );
		return true;
	}	
	
	public final void setDocumentSequences(BitSetWrapper bitSets) {
		this.bitsets = bitSets;
		this.sets = null;
	}
	
	public final void setDocumentIds(Set sets) {
		this.bitsets = null;
		this.sets = sets;
	}
	
	public final BitSetWrapper getDocumentSequences() {
		return this.bitsets ;
	}
	
	public final Set getDocumentIds() {
		return this.sets;
	}

	public final void clear() {
		if ( null != bitsets ) bitsets.clear();
		if ( null != sets ) sets.clear();
	}
	
	public final void reset() {
		if ( null != bitsets ) bitsets.clear();
		if ( null != sets ) sets.clear();
		this.isVirgin = true;
	}

	public final void and (final BitSetOrSet source) throws FederatedSearchException {
		if ( null == source) {
			if ( DEBUG_MODE ) {
				FederatedSearchLog.l.debug("\n\n Source is null. Clearning \n\n");
			}			
			this.clear();
			return;
		}
		
		if ( DEBUG_MODE ) {
			FederatedSearchLog.l.trace("Bits:Sets (AND) = " + source.toString() );
		}
		
		if ( null != source.sets) {
			if ( null == this.sets) this.sets = new HashSet<Object>(); 
			and (this.sets,source.sets);
		} else if ( null != source.bitsets) {
			if ( null == this.bitsets) this.bitsets = new BitSetWrapper(); 
			and (this.bitsets,source.bitsets);
		} else {
			this.clear();
		}
	}
	
	public final void or (final BitSetOrSet source) throws FederatedSearchException {
		if ( null == source) return;
		
		if ( null != source.bitsets) {
			if ( null == this.bitsets) this.bitsets = new BitSetWrapper(); 
			or (this.bitsets,source.bitsets);
		} else if ( null != source.sets) {
			if ( null == this.sets) this.sets = new HashSet<Object>(); 
			or (this.sets,source.sets);
		}
	}

	public final void not (final BitSetOrSet source) throws FederatedSearchException {
		if ( null == source) return;
		if ( null != source.bitsets) {
			if ( null == this.bitsets) this.bitsets = new BitSetWrapper(); 
			not (this.bitsets,source.bitsets);
		}
		else if ( null != source.sets) {
			if ( null == this.sets) this.sets = new HashSet<Object>(); 
			not (this.sets,source.sets);
		}
	}
	
	@Override
	public final String toString() {
		int bitSetsT = ( null == bitsets) ? 0 : bitsets.length();
		int setsT = ( null == sets) ? 0 : sets.size();
		return "BitSetOrSet - " + bitSetsT + "-" + setsT;
	}
	
	/************************************************************
	 * SET CALCULATION START
	 *************************************************************/
	
	private static final void and (final Set<Object> destination, final Set<Object> source) throws FederatedSearchException {
		
		if ( null == destination) {
			source.clear();
			return;
		}
		
		if ( null == source) {
			destination.clear();
			return;
		}

		int sourceT = ( null == source) ? 0 : source.size();
		int destinationT = ( null == destination) ? 0 : destination.size();

		if ( 0 == sourceT) {
			if ( INFO_MODE ) {
				FederatedSearchLog.l.info("Set<?>: and cleared bits as source is null");
			}
			destination.clear();
			return;
		}
		
		List<Object> intersected = new ArrayList<Object>();
		if ( sourceT > destinationT) {
			for (Object key : destination) {
				if ( source.contains(key)) intersected.add(key);
			}
		} else {
			for (Object key : source) {
				if ( destination.contains(key)) intersected.add(key);
			}
		}
		if ( DEBUG_MODE ) {
			FederatedSearchLog.l.info("BitSetOrSet: " + source.size() + " AND " + destination.size() + " = " + intersected.size() );
		}
		
		destination.clear();
		destination.addAll(intersected);
		
		
	}
	
	private static final void or (final Set<Object> destination, final Set<Object> source) throws FederatedSearchException {
		if ( null == destination) {
			source.clear();
		}
		
		int sourceT = ( null == source) ? 0 : source.size();
		if ( 0 == sourceT) return;
		
		destination.addAll(source);
		
		if ( DEBUG_MODE ) {
			FederatedSearchLog.l.info("BitSetOrSet: " + source.size() + " OR XX = " + destination.size() );
		}
	}

	private static final void not (final Set<Object> destination, final Set<Object> source) throws FederatedSearchException {
		if ( null == destination) {
			source.clear();
		}
		if ( null == source) {
			destination.clear();
			return;
		}

		destination.removeAll(source);
	}
		
	
	/************************************************************
	 * BITSET CALCULATION STARTS
	 *************************************************************/
	
	private static final void and (final BitSetWrapper destination, final BitSetWrapper source) throws FederatedSearchException {
		
		if ( null == destination) {
			source.clear();
		}

		if ( null == source) {
			if ( INFO_MODE ) {
				FederatedSearchLog.l.info("BitSetOrSet: and cleared bits as source is null");
			}
			destination.clear();
			return;
		}
		
		destination.and(source);
		
		if ( DEBUG_MODE ) {
			int size = ( null == destination) ? 0 : destination.size();
			FederatedSearchLog.l.info("BitSetOrSet: and result " + size);
		}
		
	}
	
	private static final void or (BitSetWrapper destination, BitSetWrapper source) throws FederatedSearchException {
		
		if ( null == destination) {
			throw new FederatedSearchException("Destination bitset is null");
		}
		if ( null == source) return;

		destination.or(source);
		
		if ( DEBUG_MODE ) {
			int size = ( null == destination) ? 0 : destination.size();
			FederatedSearchLog.l.info("BitSetOrSet: or result " + size);
		}
		
	}

	private static final void not (BitSetWrapper destination, BitSetWrapper source) throws FederatedSearchException {
		
		if ( null == destination) {
			throw new FederatedSearchException("Destination bitset is null");
		}

		if ( null == source) return;
		
		destination.andNot(source);
		
		if ( DEBUG_MODE ) {
			int size = ( null == destination) ? 0 : destination.size();
			FederatedSearchLog.l.info("BitSetOrSet: andNot result " + size);
		}
		
	}	

	/************************************************************
	 * BITSET CALCULATION ENDS
	 *************************************************************/
	
	public final boolean contains(final int key) {
		if ( null != this.sets) return this.sets.contains(key);
		else if ( null != this.bitsets ) return this.bitsets.get(key);
		else return false;
	}
	
}
