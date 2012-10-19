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

import java.util.ArrayList;
import java.util.List;

public class QueryArgs {
	public String query;
	private List<String> params = null;
	
	public QueryArgs(String query) {
		this.query = query;
	}
	
	public QueryArgs(String query, String param1) {
		System.out.println("Creating Param : " + query + "=" + param1);
		this.query = query;
		addParam(param1);
	}

	public QueryArgs(String query, String param1, String param2) {
		this.query = query;
		addParam(param1);
		addParam(param2);
	}

	public QueryArgs(String query, String param1, String param2, String param3) {
		this.query = query;
		addParam(param1);
		addParam(param2);
		addParam(param3);
	}

	public void addParam(String param) {
		if ( null == params) params = new ArrayList<String>();
		params.add(param);
	}
	
	public List<String> getParams() {
		return this.params;
	}
	
}
