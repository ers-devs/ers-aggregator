package edu.kit.aifb.cumulus.store;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import me.prettyprint.cassandra.connection.LeastActiveBalancingPolicy;
import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnDefinition;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnIndexType;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.exceptions.HectorException;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.Variable;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;
import org.semanticweb.yars2.rdfxml.RDFXMLParser;

import edu.kit.aifb.cumulus.webapp.Listener;

public abstract class AbstractCassandraRdfHector extends Store {

	protected class LoadThread extends Thread {
		private BlockingQueue<List<Node[]>> m_queue;
		private String m_cf;
		private boolean m_finished;
		private int m_id;
 		private String m_keyspace;
		
		public LoadThread(String columnFamily, int id, String keyspace) {
			m_cf = columnFamily;
			m_queue = new ArrayBlockingQueue<List<Node[]>>(5);
			m_id = id;
			m_keyspace = keyspace;
		}
		
		public void enqueue(List<Node[]> list) throws InterruptedException {
			m_queue.put(list);
		}
		
		public void setFinished(boolean finished) {
			m_finished = finished;
		}
		
		@Override
		public void run() {
			while (!m_finished || !m_queue.isEmpty()) {
				List<Node[]> list = null;
				try {
					list = m_queue.take();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
//				long start = System.currentTimeMillis();
				int tries = 10;
				while (tries >= 0) {
					try {
						batchInsert(m_cf, list, m_keyspace);
						tries = -1;
					}
					catch (Exception e) {
						_log.severe("caught " + e + " while inserting into " + m_cf + " " + list.size() + " [" + m_id + ", tries left: " + tries + "]" + e.getMessage());
						e.printStackTrace();
						tries--;
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e1) {
							e1.printStackTrace();
						}
					}
				}
//				_log.debug("[" + m_id + "] inserted " + list.size() + " in " + (System.currentTimeMillis() - start));
			}
		}
		
	}

	// this is not used only for loading data, but for running a batch of operations
	protected class RunThread extends Thread {
		private BlockingQueue<List<Node[]>> m_queue;
		private String m_cf;
		private boolean m_finished;
		private int m_id;
		private String m_keyspace;
		
		public RunThread(String columnFamily, int id, String keyspace) {
			m_cf = columnFamily;
			m_queue = new ArrayBlockingQueue<List<Node[]>>(5);
			m_id = id;
			m_keyspace = keyspace;
		}
		
		public void enqueue(List<Node[]> list) throws InterruptedException {
			m_queue.put(list);
		}
		
		public void setFinished(boolean finished) {
			m_finished = finished;
		}
		
		@Override
		public void run() {
			while (!m_finished || !m_queue.isEmpty()) {
				List<Node[]> list = null;
				try {
					list = m_queue.take();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
//				long start = System.currentTimeMillis();
				int tries = 10;
				while (tries >= 0) {
					try {
						batchRun(m_cf, list, m_keyspace);
						tries = -1;
					}
					catch (Exception e) {
						_log.severe("caught " + e + " while running a batch into " + m_cf + " " + list.size() + " [" + m_id + ", tries left: " + tries + "]" + e.getMessage()+ " " + e.getStackTrace()[0].toString() );
						e.printStackTrace();
						tries--;
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e1) {
							e1.printStackTrace();
						}
					}
				}
//				_log.debug("[" + m_id + "] inserted " + list.size() + " in " + (System.currentTimeMillis() - start));
			}
		}
		
	}
	protected static final String COL_S = "s";
	protected static final String COL_P = "p";
	protected static final String COL_O = "o";

	private final Logger _log = Logger.getLogger(this.getClass().getName());
	
	protected List<String> _cfs;
	protected Set<String> _cols;
	protected Map<String,int[]> _maps;
	// used by BulkRun (br)
	protected Map<String,int[]> _maps_br;
	// update is composed of delete+insert, thus we need different ordering for the two ops
	protected Map<String,int[]> _maps_br_update_d;
	protected Map<String,int[]> _maps_br_update_i;
   
	protected String _hosts;
	protected Cluster _cluster;
	protected int _batchSizeMB = 1;

	protected StringSerializer _ss = StringSerializer.get();
	protected BytesArraySerializer _bs = BytesArraySerializer.get();

	protected ExecuteTransactions _transactions;

	protected AbstractCassandraRdfHector(String hosts) {
		_hosts = hosts;
		_maps = new HashMap<String,int[]>();
		_maps_br = new HashMap<String,int[]>();
		_maps_br_update_d = new HashMap<String,int[]>();
		_maps_br_update_i = new HashMap<String,int[]>();
		_cfs = new ArrayList<String>();
		_cols = new HashSet<String>();
		_cols.add(COL_S);
		_cols.add(COL_P);
		_cols.add(COL_O);
		_transactions = new ExecuteTransactions();
		_log.info("cassandrardfhector class: " + getClass().getCanonicalName());
	}

	@Override
	public void open() throws StoreException {
		CassandraHostConfigurator config = new CassandraHostConfigurator(_hosts);
		config.setCassandraThriftSocketTimeout(60*1000);
		//config.setMaxActive(6);
		//config.setExhaustedPolicy(ExhaustedPolicy.WHEN_EXHAUSTED_BLOCK);
		config.setRetryDownedHostsDelayInSeconds(5);
		config.setRetryDownedHostsQueueSize(128);
		config.setRetryDownedHosts(true);
		
		// experiments with timeouts
		config.setCassandraThriftSocketTimeout(0);
		config.setMaxWaitTimeWhenExhausted(-1);
		
		// RoundRobin, LeastActive and Dynamic as possible values
		//config.setLoadBalancingPolicy(new RoundRobinBalancingPolicy());
		config.setLoadBalancingPolicy(new LeastActiveBalancingPolicy());
		_cluster = HFactory.getOrCreateCluster("CassandraRdfHectorHierHash", config);
		_log.finer("connected to " + _hosts);
	}

	// pass the exact name of the keyspace created previously
	public boolean existsKeyspace(String keyspaceName) { 
		if( _cluster.describeKeyspace(keyspaceName) != null ) 
			return true;
		return false;
	}

	// don't use the encoded keyspace; this is used by Listener for initialization
	public int createKeyspaceInit(String keyspaceName) {
		if (! existsKeyspace(keyspaceName))  {
			try { 
				_cluster.addKeyspace(createKeyspaceDefinition(keyspaceName));	
			} catch( HectorException ex ) { 
				ex.printStackTrace(); 	
				return 3;
			}	
		}
		else 
			return 1;
		HFactory.createKeyspace(keyspaceName, _cluster, Listener.DEFAULT_CONSISTENCY_POLICY);
		return 0;
	}

	// create a keyspace if it does not exist yet and add 
	//NOTE: pass NON-encoded keyspace
	public int createKeyspace(String keyspaceName) {
		String encoded_keyspaceName = Store.encodeKeyspace(keyspaceName);
		if (! existsKeyspace(encoded_keyspaceName)) 
			try { 
				_cluster.addKeyspace(createKeyspaceDefinition(encoded_keyspaceName));	
			} catch( HectorException ex ) { 
				ex.printStackTrace(); 	
				_log.severe("ERS exception: "+ex.getMessage() );
				return 3;
			}	
		else 
			return 1;
		HFactory.createKeyspace(encoded_keyspaceName, _cluster, Listener.DEFAULT_CONSISTENCY_POLICY);
		// also add an entry into Listener.GRAPHS_NAMES_KEYSPACE to keep a mapping of hashed keyspace name and the real one 
		this.addData("\""+encoded_keyspaceName+"\"", "\"hashValue\"", "\""+keyspaceName+"\"", Listener.GRAPHS_NAMES_KEYSPACE);	
		return 0;
	}

	// get all keyspaces' names
	public List<String> getAllKeyspaces() {
		List<String> res = new ArrayList<String>();
		for (KeyspaceDefinition ksDef : _cluster.describeKeyspaces()) {
			if (!ksDef.getName().startsWith("system"))
				res.add(ksDef.getName());
		}
		return res;
	}

	protected Keyspace getExistingKeyspace(String keyspace) { 
		return HFactory.createKeyspace(keyspace, _cluster);
	}

	// pass the actual name of the keyspace as it was created before
	public boolean emptyKeyspace(String keyspace) { 
		try { 
			int r = queryEntireKeyspace(keyspace, new PrintWriter("/dev/null"), 1);
			if( r == 0 ) 
				// not even one record, thus we assume it is emtpy 
				return true;
		}
		catch( Exception ex ) { 
			ex.printStackTrace(); 
			// if an exception rise, better to consider the keyspace empty
			return true;
		}
		return false;
		
	}

	// drop a keyspace if it exists (if force is true, delete even if it is not empty)
	public int dropKeyspace(String keyspaceName, boolean force) { 
		if(keyspaceName.startsWith("system")) 
			return 3;
		if ( !existsKeyspace(keyspaceName))
			return 1;
	
		if ( !force ) {
			// test first of all if the keyspace / graph is empty 
			if( ! emptyKeyspace(keyspaceName) ) 
				return 4;
		}
		try { 
			_cluster.dropKeyspace(keyspaceName);
		} catch(HectorException ex) { 
			ex.printStackTrace(); 
			return 2;
		}
		return 0;
	}

	protected ColumnDefinition createColDef(String colName, String validationClass, boolean indexed) {
		return createColDef(colName, validationClass, indexed, colName + "_index");
	}
	
	protected ColumnDefinition createColDef(String colName, String validationClass, boolean indexed, String indexName) {
		BasicColumnDefinition colDef = new BasicColumnDefinition();
		colDef.setName(_ss.toByteBuffer(colName));
		colDef.setValidationClass(validationClass);
		if (indexed) {
			colDef.setIndexType(ColumnIndexType.KEYS);
			colDef.setIndexName(indexName);
		}
		return colDef;
	}

	public void setBatchSize(int batchSizeMB) {
		_batchSizeMB = batchSizeMB;
	}
	
	protected KeyspaceDefinition createKeyspaceDefinition(String keyspaceName) {
		return HFactory.createKeyspaceDefinition(keyspaceName, "org.apache.cassandra.locator.SimpleStrategy", 
			Listener.DEFAULT_REPLICATION_FACTOR, createColumnFamiliyDefinitions(keyspaceName));
	}

	protected abstract List<ColumnFamilyDefinition> createColumnFamiliyDefinitions(String keyspaceName);

	protected ColumnFamilyDefinition createCfDefFlat(String cfName, List<String> cols, List<String> indexedCols, ComparatorType keyComp, String keyspaceName) {
		BasicColumnFamilyDefinition cfdef = new BasicColumnFamilyDefinition();
		cfdef.setKeyspaceName(keyspaceName);
		cfdef.setName(cfName);
		cfdef.setColumnType(ColumnType.STANDARD);
		cfdef.setComparatorType(ComparatorType.UTF8TYPE);
		cfdef.setKeyValidationClass(keyComp.getClassName());
		cfdef.setDefaultValidationClass(ComparatorType.UTF8TYPE.getClassName());

		Map<String,String> compressionOptions = new HashMap<String, String>();
		compressionOptions.put("sstable_compression", "SnappyCompressor");
		cfdef.setCompressionOptions(compressionOptions);

		if (cols != null)
			for (String colName : cols)
				cfdef.addColumnDefinition(createColDef(colName, ComparatorType.UTF8TYPE.getClassName(), indexedCols.contains(colName), "index_" + colName.substring(1)));
		
		return new ThriftCfDef(cfdef);
	}
	
	protected ColumnFamilyDefinition createCfDefHier(String cfName, String keyspaceName) {
		BasicColumnFamilyDefinition cfdef = new BasicColumnFamilyDefinition();
		cfdef.setKeyspaceName(keyspaceName);
		cfdef.setName(cfName);
		cfdef.setColumnType(ColumnType.SUPER);
		cfdef.setComparatorType(ComparatorType.UTF8TYPE);
		Map<String,String> compressionOptions = new HashMap<String, String>();
		compressionOptions.put("sstable_compression", "SnappyCompressor");
		cfdef.setCompressionOptions(compressionOptions);
		return new ThriftCfDef(cfdef);
	}
	
	protected abstract void batchInsert(String cf, List<Node[]> li, String keyspace);
	protected abstract void batchDelete(String cf, List<Node[]> li, String keyspace);
	protected abstract void batchDeleteRow(String cf, List<Node[]> li, String keyspace);
	protected abstract void batchRun(String cf, List<Node[]> li, String keyspace);

	protected boolean isVariable(Node n) {
		return n instanceof Variable;
	}

	@Override
	public void close() throws StoreException {
		_cluster.getConnectionManager().shutdown();
	}

	public int runTransaction(Transaction t) { 
		return _transactions.executeTransaction(t, this);
	}

	@Override
	public int addData(Iterator<Node[]> it, String keyspace) throws StoreException {
		// check firstly if keyspace exists, if not, return error 
		if( !existsKeyspace(keyspace) ) { 
			return -1;
		} 
		List<Node[]> batch = new ArrayList<Node[]>();
		int batchSize = 0;
		int count = 0;
		while (it.hasNext()) {
			Node[] nx = it.next();
			//_log.info("addData  " + nx[0].toString() + " " + nx[1].toString() + " " + nx[2].toString());
			batch.add(nx);
         // add the back link as well 
         if( Listener.DEFAULT_ERS_CREATE_LINKS.equals("yes") && nx[2] instanceof Resource ) 
            batch.add(Util.reorderForLink(nx, _maps.get("link")));

			batchSize += nx[0].toN3().getBytes().length + nx[1].toN3().getBytes().length + nx[2].toN3().getBytes().length;
			count++;
			if (batchSize >= _batchSizeMB * 1048576) {
				_log.finer("insert batch of size " + batchSize + " (" + batch.size() + " tuples)");
				for (String cf : _cfs)
					batchInsert(cf, batch, keyspace);
				batch = new ArrayList<Node[]>();
				batchSize = 0;
			}
		}
		if (batch.size() > 0)
			for (String cf : _cfs)
				batchInsert(cf, batch, keyspace);
		return count;
	}

	// TM: add just one record
	public int addData(String e, String p, String v, String keyspace) {
		if( !existsKeyspace(keyspace) ) { 
			return -2; 
		}
		String triple = e + " " + p + " " + v + " ."; 
		try { 
			Node[] nx = NxParser.parseNodes(triple);	
	 		List<Node[]> batch = new ArrayList<Node[]>();
			batch.add(nx);
                        // add the back link as well
                        if( Listener.DEFAULT_ERS_CREATE_LINKS.equals("yes") && nx[2] instanceof Resource )
                            batch.add(Util.reorderForLink(nx, _maps.get("link")));
			for (String cf : _cfs) {
				batchInsert(cf, batch, keyspace);
			}
			return 0;
		} 
		catch( ParseException ex ) { 
			ex.printStackTrace();
			_log.severe(ex.getMessage());
			return -1;
		}
	}

	// TM: delete a record 
	public int deleteData(String e, String p, String v, String keyspace) { 
		// check if keyspace exists	
		if( !existsKeyspace(keyspace)) { 
			return -2; 
		}
		String triple = e + " " + p + " " + v + " ."; 
		try { 
			Node[] nx = NxParser.parseNodes(triple);
                        this.deleteData(nx, keyspace);
                        // add the back link as well
                        if( Listener.DEFAULT_ERS_CREATE_LINKS.equals("yes")
                                && nx[2] instanceof Resource )
                            this.deleteData(Util.reorderForLink(nx, _maps.get("link")),
                                    keyspace);
			return 0;
		} 
		catch( ParseException ex ) { 
			ex.printStackTrace();
			_log.severe(ex.getMessage());
			return -1;
		}
	}
	
	public void deleteData(Node[] nx, String keyspace) { 
	 	List<Node[]> batch = new ArrayList<Node[]>();
		batch.add(nx);
                // add the back link as well
                if( Listener.DEFAULT_ERS_CREATE_LINKS.equals("yes")
                        && nx[2] instanceof Resource )
                    batch.add(Util.reorderForLink(nx, _maps.get("link")));
		for (String cf : _cfs) {
			batchDelete(cf, batch, keyspace);
		}
	}

	// return one row iterator over all its columns  
	public Iterator<Node[]> getRowIterator(String e, String keyspace) { 
		 Resource resource = new Resource(e);
		try {
			Node[] query = new Node[3];
			query[0] = new Literal(e);
			query[1] = new Variable("p");
			query[2] = new Variable("o");	
       	        	return this.query(query, Integer.MAX_VALUE, keyspace); 
		}
                catch (StoreException ex) {
			ex.printStackTrace();
			_log.severe("ERS exception: " + ex.getMessage());
                        return null; 
		}
	}

	// it queries to get the whole data and then it uses it to delete (of course, it would be easier to directly delete it by using the row key, but for POS and OSP column families it is not that easy)
	public int deleteByRowKey(String e, String keyspace) { 
	 	// check if keyspace exists	
		if( !existsKeyspace(keyspace)) { 
			return -2; 
		}
		Iterator<Node[]> it = getRowIterator(e, keyspace);		
		for( ; it.hasNext(); ) {
			Node[] n = (Node[])it.next();
			this.deleteData(n, keyspace);
		} 
		return 1;
	}


	/* TM: update just one record
         * NOTE: this has a bit of overhead since the value is stored as column name; thus 
         * for updating it needs 1 deletion and 1 insertion 
  	*/
	public int updateData(String e, String p, String v_old, String v_new, String keyspace) { 
		// check if keyspace exists
		if ( !existsKeyspace(keyspace) ) { 
			return -2; 
		}
		// assuming the old record exists, run a del
		if( deleteData(e, p, v_old, keyspace) == -1 )
			return -1;
		// now run an add
	 	return addData(e, p, v_new, keyspace);	
	}

	@Override
	public boolean contains(Node s, String keyspace) throws StoreException {
		return query(new Node[] { s, new Variable("p"), new Variable("o") }, keyspace).hasNext();
	}

	@Override
	public Iterator<Node[]> query(Node[] query, String keyspace) throws StoreException {
		return query(query, Integer.MAX_VALUE, keyspace);
	}

	private Node urlDecode(Node n) {
		if (n instanceof Resource)
			try {
				return new Resource(URLDecoder.decode(n.toString(), "UTF-8"));
			} catch (UnsupportedEncodingException e) {
				_log.severe(n.toN3() + " " + e.getMessage());
				return n;
			}
		return n;
	}

        public static String getLN() {
    return String.valueOf(Thread.currentThread().getStackTrace()[2].getLineNumber());
}

	// parses input file, creates the RunThread/s and waits for them to finish
	public void bulkRun(InputStream fis, String format, String columnFamily, int threadCount, String keyspace) throws IOException, InterruptedException {
		_log.info("run batch for CF " + columnFamily);
		List<RunThread> threads = new ArrayList<RunThread>();
		if (threadCount < 0) {
			//threadCount = Math.max(1, (int)(_cluster.getConnectionManager().getHosts().size() / 1.5));
			threadCount = Math.max(1, (int)(_cluster.getConnectionManager().getHosts().size() / 1.0));
		}
		// start here the RunThread which will do the batches 
		for (int i = 0; i < threadCount; i++) {
			RunThread t = new RunThread(columnFamily, i, keyspace);
			threads.add(t);
			t.start(); 
		}
		_log.info("created " + threads.size() + " running batch threads");
		int curThread = 0;
		
		Iterator<Node[]> nxp = null;
		nxp = new NxParser(fis);
		List<Node[]> operations = new ArrayList<Node[]>();
		
		long start = System.currentTimeMillis();
		int i = 0;
		int batchSize = 0;
		long data = 0;
		int cnt=0;
		while (nxp.hasNext()) {
			Node[] nx = nxp.next();
			if( nx.length != 5 ) {
				++cnt;
				continue;
			}
			if (nx[2].toN3().length() + nx[1].toN3().length() > 64000) {
				_log.info("skipping too large row (max row size: 64k");
				continue;
			}
			operations.add(nx);
			i++;
					 //do not count 'graph' and 'query_type'
			for (int k=0; k < nx.length-2; k++) {
				batchSize += nx[k].toN3().getBytes().length; // + nx[1].toN3().getBytes().length + nx[2].toN3().getBytes().length;
			}
			if (batchSize >= _batchSizeMB * 1048576) {
				_log.info("batch ready: " + operations.size() + " triples, size: " + batchSize + ", thread: " + curThread);
				data += batchSize;
				threads.get(curThread).enqueue(operations);
				operations = new ArrayList<Node[]>();
				batchSize = 0;
				curThread = (curThread + 1) % threads.size();
			}
			if (i % 200000 == 0)
				_log.info(i + " into " + columnFamily + " in " +  (System.currentTimeMillis() - start) + " ms (" + ((double)i / (System.currentTimeMillis() - start) * 1000) + " quads/s) (" + ((double)data / 1000 / (System.currentTimeMillis() - start) * 1000) + " kbytes/s)"); 
		}
		_log.info("NO OF NOT LOADED QUADS DUE TO PARSING ERROR: " + cnt);
		if (operations.size() > 0) {
			threads.get(curThread).enqueue(operations);
		}
		_log.info("waiting for threads to finish....");
		for (RunThread t : threads) {
			t.setFinished(true);
			t.enqueue(new ArrayList<Node[]>());
			t.join();
		}
		long time = (System.currentTimeMillis() - start);
		_log.info(i + " triples inserted into " + columnFamily + " in " + time + " ms (" + ((double)i / time * 1000) + " triples/s)");
	}

	// used by BatchRun servlet to load a given file of operations (not only inserts)
	public int bulkRun(File file, String format, int threads, String keyspace) throws StoreException, IOException {
		try {
			// check firstly if keyspace exists, if not, return error 
			if( !existsKeyspace(keyspace) ) { 
				return 1;
			} 
			// run the batch for each column family
			for (String cf : _cfs) {
				FileInputStream fis = new FileInputStream(file);
				bulkRun(fis, format, cf, threads, keyspace);
				fis.close();
			}			
		} catch (InterruptedException e) {
			throw new StoreException(e);
		} catch (IOException e) {
			throw new StoreException(e);
		}
		return 0;
	}

	public void batchBulkLoad(InputStream fis, String format, String columnFamily, int threadCount, String keyspace) throws IOException, InterruptedException {
		_log.info("bulk loading " + columnFamily);
		List<LoadThread> threads = new ArrayList<LoadThread>();
		if (threadCount < 0) {
			//threadCount = Math.max(1, (int)(_cluster.getConnectionManager().getHosts().size() / 1.5));
			threadCount = Math.max(1, (int)(_cluster.getConnectionManager().getHosts().size() / 1.0));
		}

		for (int i = 0; i < threadCount; i++) {
			LoadThread t = new LoadThread(columnFamily, i, keyspace);
			threads.add(t);
			t.start();
		}
		_log.info("created " + threads.size() + " loading threads");
		int curThread = 0;
		
		Iterator<Node[]> nxp = null;
		if (format.equals("nt") || format.equals("nq")) {
			nxp = new NxParser(fis);
		}
		else if (format.equals("xml")) {
			try {
				nxp = new RDFXMLParser(fis, "http://example.org");
			}
			catch (ParseException e) {
				e.printStackTrace();
				_log.severe(e.getMessage());
				throw new IOException(e);
			}
		}
		List<Node[]> triples = new ArrayList<Node[]>();
		
		long start = System.currentTimeMillis();
		int i = 0;
		int batchSize = 0;
		long data = 0;
		while (nxp.hasNext()) {
			Node[] nx = nxp.next();
			if (nx[2].toN3().length() + nx[1].toN3().length() > 64000) {
				_log.info("skipping too large row (max row size: 64k");
				continue;
			}
			triples.add(nx);
         // add the back link as well 
         if( Listener.DEFAULT_ERS_CREATE_LINKS.equals("yes") && nx[2] instanceof Resource ) 
            triples.add(Util.reorderForLink(nx, _maps.get("link")));
			i++;
			for (int k=0; k < nx.length; k++) {
				batchSize += nx[k].toN3().getBytes().length; // + nx[1].toN3().getBytes().length + nx[2].toN3().getBytes().length;
			}
			if (batchSize >= _batchSizeMB * 1048576) {
				_log.finer("batch ready: " + triples.size() + " triples, size: " + batchSize + ", thread: " + curThread);
				data += batchSize;
				threads.get(curThread).enqueue(triples);
				triples = new ArrayList<Node[]>();
				batchSize = 0;
				curThread = (curThread + 1) % threads.size();
			}
			if (i % 200000 == 0)
				_log.info(i + " into " + columnFamily + " in " +  (System.currentTimeMillis() - start) + " ms (" + ((double)i / (System.currentTimeMillis() - start) * 1000) + " triples/s) (" + ((double)data / 1000 / (System.currentTimeMillis() - start) * 1000) + " kbytes/s)");
		}
		if (triples.size() > 0) {
			threads.get(curThread).enqueue(triples);
		}
		_log.info("waiting for threads to finish....");
		for (LoadThread t : threads) {
			t.setFinished(true);
			t.enqueue(new ArrayList<Node[]>());
			t.join();
		}
		long time = (System.currentTimeMillis() - start);
		_log.info(i + " triples inserted into " + columnFamily + " in " + time + " ms (" + ((double)i / time * 1000) + " triples/s)");
	}

	public void bulkLoad(File file, String format, String keyspace) throws StoreException, IOException {
		bulkLoad(file, format, -1, keyspace);
	}
	
	// called by BulkLoad servet 
	public int bulkLoad(File file, String format, int threads, String keyspace) throws StoreException, IOException {
		try {
			// check firstly if keyspace exists, if not, return error 
			if( !existsKeyspace(keyspace) ) { 
				return 1;
			} 
			for (String cf : _cfs) {
				FileInputStream fis = new FileInputStream(file);
				batchBulkLoad(fis, format, cf, threads, keyspace);
				fis.close();
			}			
		} catch (InterruptedException e) {
			throw new StoreException(e);
		} catch (IOException e) {
			throw new StoreException(e);
		}
		return 0;
	}

	public void bulkLoad(File file, String format, String cf, String keyspace) throws StoreException, IOException {
		bulkLoad(file, format, cf, -1, keyspace);
	}

	public void bulkLoad(File file, String format, String cf, int threads, String keyspace) throws StoreException {
		try {
			FileInputStream fis = new FileInputStream(file);
			batchBulkLoad(fis, format, cf, threads, keyspace);
			fis.close();
		} catch (InterruptedException e) {
			throw new StoreException(e);
		} catch (IOException e) {
			throw new StoreException(e);
		}
	}

	public void bulkLoad(InputStream is, String format, String cf, String keyspace) throws StoreException, IOException {
		bulkLoad(is, format, cf, -1, keyspace);
	}

	public void bulkLoad(InputStream is, String format, String cf, int threads, String keyspace) throws StoreException {
		try {
			batchBulkLoad(is, format, cf, threads, keyspace);
		} catch (InterruptedException e) {
			throw new StoreException(e);
		} catch (IOException e) {
			throw new StoreException(e);
		}
	}
	
	public String getStatus() {
		StringBuffer sb = new StringBuffer();
		
		sb.append("Connected to cluster: ");
		sb.append(_cluster.getConnectionManager().getClusterName());
		sb.append('\n');

		sb.append("Status per pool:\n");
		
		for (String s : _cluster.getConnectionManager().getStatusPerPool()) {
			sb.append(s);
			sb.append('\n');
		}
		
		return sb.toString();
	}
}
