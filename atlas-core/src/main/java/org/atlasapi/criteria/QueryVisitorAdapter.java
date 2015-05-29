/* Copyright 2009 Meta Broadcast Ltd

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. */

package org.atlasapi.criteria;



/**
 * Base class providing a default walk of the query AST. Extend and override
 * to customise visitor behaviour.
 *  
 * @author Robert Chatley (robert@metabroadcast.com)
 */
public abstract class QueryVisitorAdapter<V> implements QueryVisitor<V> {
	
	@Override
	public V visit(IntegerAttributeQuery query) {
		return defaultValue(query);
	}

	@Override
	public V visit(StringAttributeQuery query) {
		return defaultValue(query);
	}

	@Override
	public V visit(DateTimeAttributeQuery dateTimeAttributeQuery) {
		return defaultValue(dateTimeAttributeQuery);
	}

	@Override
	public V visit(EnumAttributeQuery<?> query) {
		return defaultValue(query);
	}
	
	@Override
	public V visit(BooleanAttributeQuery query) {
		return defaultValue(query);
	}
	
	@Override
	public V visit(MatchesNothing nothing) {
		return defaultValue(nothing);
	}
	
	@Override
	public V visit(IdAttributeQuery query) {
	    return defaultValue(query);
	}
	
	@Override
	public V visit(FloatAttributeQuery query) {
	    return defaultValue(query);
	}
	
	protected V defaultValue(AtomicQuery query) {
		return null;
	}

	@Override public V visit(SortAttributeQuery query) {
		return defaultValue(query);
	}
}