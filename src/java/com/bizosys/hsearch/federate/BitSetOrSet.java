package com.bizosys.hsearch.federate;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class BitSetOrSet {
	
	private static boolean DEBUG_MODE = FederatedSearchLog.l.isDebugEnabled();
	private static boolean INFO_MODE = FederatedSearchLog.l.isInfoEnabled();

	private BitSet bits = null;
	private Set sets = null;
	
	public int size() {
		
		if ( null != bits) {
			int size = 0;
			for (int i = bits.nextSetBit(0); i >= 0; i = bits.nextSetBit(i+1)) size++;
			return size;
		}
		
		if ( null != sets) return sets.size();
		
		return 0;
	}
	
	public boolean isEmpty() {
		if ( null != bits) return bits.isEmpty();
		if ( null != sets) return ( sets.size() == 0 );
		return true;
	}	
	
	public void setDocumentSequences(BitSet bitSets) {
		this.bits = bitSets;
		this.sets = null;
	}
	
	public void setDocumentIds(Set sets) {
		this.bits = null;
		this.sets = sets;
	}
	
	public BitSet getDocumentSequences() {
		return this.bits;
	}
	
	public Set getDocumentIds() {
		return this.sets;
	}

	public void clear() {
		if ( null == bits ) bits.clear();
		if ( null == sets ) sets.clear();
	}
	
	public void and (BitSetOrSet source) throws FederatedSearchException {
		if ( null == source) this.clear();
		if ( null != source.bits) {
			if ( null == this.bits) this.bits = new BitSet(); 
			and (this.bits,source.bits);
		} else {
			if ( null == this.sets) this.sets = new HashSet<Object>(); 
			and (this.sets,source.sets);
		}
	}
	
	public void or (BitSetOrSet source) throws FederatedSearchException {
		if ( null == source) return;
		if ( null != source.bits) {
			if ( null == this.bits) this.bits = new BitSet(); 
			or (this.bits,source.bits);
		} else {
			if ( null == this.sets) this.sets = new HashSet<Object>(); 
			or (this.sets,source.sets);
		}
	}

	public void not (BitSetOrSet source) throws FederatedSearchException {
		if ( null == source) return;
		if ( null != source.bits) {
			if ( null == this.bits) this.bits = new BitSet(); 
			not (this.bits,source.bits);
		}
		else {
			if ( null == this.sets) this.sets = new HashSet<Object>(); 
			not (this.sets,source.sets);
		}
	}
	
	@Override
	public String toString() {
		int bitSetsT = ( null == bits) ? 0 : bits.length();
		int setsT = ( null == sets) ? 0 : sets.size();
		return "BitSetOrSet - " + bitSetsT + "-" + setsT;
	}
	
	/************************************************************
	 * SET CALCULATION START
	 *************************************************************/
	
	private static final void and (Set<Object> destination, Set<Object> source) throws FederatedSearchException {
		
		if ( null == destination) {
			throw new FederatedSearchException("Destination set is null");
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
		
		destination.clear();
		destination.addAll(intersected);
		
		if ( DEBUG_MODE ) {
			FederatedSearchLog.l.info("BitSetOrSet: and result " + destination.toString());
		}
		
	}
	
	private static final void or (Set<Object> destination, Set<Object> source) throws FederatedSearchException {
		if ( null == destination) {
			throw new FederatedSearchException("Destination set is null");
		}
		
		int sourceT = ( null == source) ? 0 : source.size();
		if ( 0 == sourceT) return;
		
		destination.addAll(source);
		
		if ( DEBUG_MODE ) {
			FederatedSearchLog.l.info("BitSetOrSet: and result " + destination.toString());
		}		
	}

	private static final void not (Set<Object> destination, Set<Object> source) throws FederatedSearchException {
		if ( null == destination) {
			throw new FederatedSearchException("Destination set is null");
		}
		destination.removeAll(source);
	}
		
	
	/************************************************************
	 * BITSET CALCULATION STARTS
	 *************************************************************/
	
	private static final void and (BitSet destination, BitSet source) throws FederatedSearchException {
		
		if ( null == destination) {
			throw new FederatedSearchException("Destination bitset is null");
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
			FederatedSearchLog.l.info("BitSetOrSet: and result " + destination.toString());
		}
		
	}
	
	private static final void or (BitSet destination, BitSet source) throws FederatedSearchException {
		
		if ( null == destination) {
			throw new FederatedSearchException("Destination bitset is null");
		}
		if ( null == source) return;

		destination.or(source);
		
		if ( DEBUG_MODE ) {
			FederatedSearchLog.l.info("BitSetOrSet: or result " + destination.toString());
		}
		
	}

	private static final void not (BitSet destination, BitSet source) throws FederatedSearchException {
		
		if ( null == destination) {
			throw new FederatedSearchException("Destination bitset is null");
		}

		if ( null == source) return;
		
		destination.andNot(source);
		
		if ( DEBUG_MODE ) {
			FederatedSearchLog.l.info("BitSetOrSet: andNot result " + destination.toString());
		}
		
	}	

	/************************************************************
	 * BITSET CALCULATION ENDS
	 *************************************************************/
	
}
