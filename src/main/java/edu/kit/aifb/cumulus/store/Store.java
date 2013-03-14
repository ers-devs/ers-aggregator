package edu.kit.aifb.cumulus.store;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.Variable;

/** 
 * 
 * @author aharth
 */
public abstract class Store {
	
	public class DescribeIterator implements Iterator<Node[]> {

		private Resource m_resource;
		private boolean m_include2Hop;

		// iterator for pattern with resource as subject
		private Iterator<Node[]> m_sit;
		// iterator for hop patterns
		private Iterator<Node[]> m_hit;
		// iterator for pattern with resource as object
		private Iterator<Node[]> m_oit;
		private int m_subjects;
		private int m_objects;
		private String m_keyspace; 
		
		public DescribeIterator(Resource resource, boolean include2Hop, int subjects, int objects, String keyspace) throws StoreException {
			m_resource = resource;
			m_include2Hop = include2Hop;
			m_subjects = subjects;
			m_objects = objects;
			m_keyspace = keyspace;
			
			// start with pattern with resource as subjectk
			m_sit = query(pattern(resource, null, null), m_subjects, keyspace);
		}
		
		@Override
		public boolean hasNext() {
			if (m_sit.hasNext())
				return true;
			
			if (m_hit != null && m_hit.hasNext())
				return true;
			
			if (m_oit != null && m_oit.hasNext())
				return true;
			
			return false;
		}

		@Override
		public Node[] next() {
			if (m_include2Hop && m_hit != null && m_hit.hasNext())
				return m_hit.next();
			
			if (m_sit.hasNext()) {
				Node[] next = m_sit.next();
				
				// if the hop should be included, prime the hop iterator,
				// pattern has the current object as subject
				if (m_include2Hop) {
					try {
						m_hit = query(pattern(next[2], null, null), m_subjects, m_keyspace);
					} catch (StoreException e) {
						e.printStackTrace();
						m_hit = null;
					}
				}
				
				// when the subject iterator is finished and there are no more
				// triples in the hop iterator, get the object iterator
				if (!m_sit.hasNext() && (!m_include2Hop || !m_hit.hasNext())) {
					try {
						m_oit = query(pattern(null, null, m_resource), m_objects, m_keyspace);
					} catch (StoreException e) {
						e.printStackTrace();
						m_oit = null;
					}
				}
				
				return next;
			}
			
			if (m_oit != null && m_oit.hasNext())
				return m_oit.next();
			
			return null;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("remove not supported");
		}
		
	}
	
	transient private final Logger _log = Logger.getLogger(this.getClass().getName());
	
	public abstract void open() throws StoreException;
	public abstract void close() throws StoreException;
	public abstract int addData(Iterator<Node[]> it, String keyspace) throws StoreException;

	// TM; TODO: add keyspace info ... 
	// add (e,p,v,g)
	public abstract int addData(String e, String p, String v, String keyspace);
	// update/replace (e,p,v_old,g) with (e,p,v_new,g);
	public abstract int updateData(String e, String p, String v_old, String v_new, String keyspace);
	// delete (e,p,v,g)  
	public abstract int deleteData(String e, String p, String v, String keyspace);
	// delete all data 
	public abstract int dropKeyspace(String keyspace);

	public abstract boolean contains(Node s, String keyspace) throws StoreException;
	
	public abstract Iterator<Node[]> query(Node[] query, String keyspace) throws StoreException;
	public abstract Iterator<Node[]> query(Node[] query, int limit, String keyspace) throws StoreException;
	
	public abstract String getStatus();
	
	public Iterator<Node[]> describe(Resource resource, boolean include2Hop, String keyspace) throws StoreException {
		return describe(resource, include2Hop, -1, -1, keyspace);
	}
	
	public Iterator<Node[]> describe(Resource resource, boolean include2Hop, int subjects, int objects, String keyspace) throws StoreException {
		return new DescribeIterator(resource, include2Hop, subjects, objects, keyspace);
	}
	
	private Node[] pattern(Node s, Node p, Node o) {
		return new Node[] { s == null ? new Variable("s") : s, p == null ? new Variable("p") : p, o == null ? new Variable("o") : o }; 
	}
}
