/*
* Copyright 2010 Bizosys Technologies Limited
*
* Licensed to the Bizosys Technologies Limited (Bizosys) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The Bizosys licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.bizosys.hsearch.federate;

import java.util.HashMap;
import java.util.Map;

public final class QueryPart {

	public String aStmtOrValue;
	private Map<String, Object> paramsForStmt = null;
	
	public QueryPart(final String query) {
		this.aStmtOrValue = query;
	}
	
	public QueryPart(final String query, final String aName, final Object value) {
		this(query);
		setParam ( aName, value);
	}

	public QueryPart(final String query, final Map<String, Object> paramsForStmt) {
		this.aStmtOrValue = query;
		this.paramsForStmt = paramsForStmt;
	}

	public final Map<String, Object> getParams() {
		return this.paramsForStmt;
	}
	
	public final void setParam ( final String name, final Object value){
		if ( null == paramsForStmt) paramsForStmt = new HashMap<String, Object>();
		paramsForStmt.put(name, value);
	}
	
}
