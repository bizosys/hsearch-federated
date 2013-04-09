package com.bizosys.hsearch.federate;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.util.Version;

/**
 * Use Lucene Parser to parse the Query
 * @author Abinasha Karana, Bizosys
 */
public final  class HQueryParser {

	public final HQuery parse(final String query) throws FederatedSearchException {
		
		WhitespaceAnalyzer analyzer = new WhitespaceAnalyzer(Version.LUCENE_35);

		try {
			
			Query qp = null;
			qp = new QueryParser(Version.LUCENE_35,"", analyzer).parse(query);
			HQuery hQuery = new HQuery();
			parseComposites(qp, hQuery);
			return hQuery;
			
		} catch (ParseException ex) {
			throw new FederatedSearchException(ex);
		}
		
	}
	
	private final void parseComposites(final Query lQuery, final HQuery hQuery) throws FederatedSearchException {

		if(lQuery instanceof TermQuery)
		{
			populateTerm(hQuery, false, true, lQuery);
			return;
		}
		
		for (BooleanClause clause : ((BooleanQuery)lQuery).clauses()) {
			
			Query subQueryL = clause.getQuery();

			if ( subQueryL instanceof BooleanQuery ) {

				HQuery subQueryH = new HQuery();
				subQueryH.isShould = clause.getOccur().compareTo(Occur.SHOULD) == 0; 
				subQueryH.isMust = clause.getOccur().compareTo(Occur.MUST) == 0;

				hQuery.subQueries.add(subQueryH);
				parseComposites(subQueryL, subQueryH);
			
			} else {
				boolean isShould = clause.getOccur().compareTo(Occur.SHOULD) == 0; 
				boolean isMust = clause.getOccur().compareTo(Occur.MUST) == 0;
				populateTerm(hQuery, isShould, isMust, subQueryL);
			}
		}
	}

	private final void populateTerm(final HQuery hQuery, final boolean isShould, final boolean isMust, final Query subQueryL) 
		throws FederatedSearchException {
		
		HTerm hTerm = new HTerm();
		hTerm.isShould = isShould; 
		hTerm.isMust = isMust;
		hTerm.boost = subQueryL.getBoost();
		hQuery.terms.add(hTerm);

		if ( subQueryL instanceof TermQuery ) {
			TermQuery lTerm = (TermQuery)subQueryL;
			hTerm.type = lTerm.getTerm().field();
			hTerm.text = lTerm.getTerm().text();

		} else if ( subQueryL instanceof FuzzyQuery ) {
			FuzzyQuery lTerm = (FuzzyQuery) subQueryL;
			hTerm.isFuzzy = true;
			hTerm.type = lTerm.getTerm().field();
			hTerm.text = lTerm.getTerm().text();
		
		} else if ( subQueryL instanceof TermRangeQuery) {
			TermRangeQuery lTerm = (TermRangeQuery) subQueryL;
			hTerm.isFuzzy = false;
			hTerm.type = lTerm.getField();
			hTerm.minRange =  lTerm.getLowerTerm();
			hTerm.maxRange = lTerm.getUpperTerm();

		} else {
			throw new FederatedSearchException(
				"HQueryParser: Not Implemented Query :" + subQueryL.getClass().toString());
		}
	}
}
