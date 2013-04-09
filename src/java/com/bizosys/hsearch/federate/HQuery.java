package com.bizosys.hsearch.federate;

import java.util.ArrayList;
import java.util.List;

/**
 * Lucene Query Population
 * @author Abinasha Karana, Bizosys
 */

public final class HQuery {

	protected boolean isShould = false;
	protected boolean isMust = false;
	protected float boost = 1.0f;
	
	public List<HQuery> subQueries = new ArrayList<HQuery>();
	public List<HTerm> terms = new ArrayList<HTerm>();
	

	public final void toTerms(final HQuery query, final List<HTerm> allTerms) {
		for (HQuery subQuery : query.subQueries) {
			toTerms(subQuery, allTerms);
		}
		allTerms.addAll(query.terms);
	}
	
	public final String toString(final String level) {
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
		
}
