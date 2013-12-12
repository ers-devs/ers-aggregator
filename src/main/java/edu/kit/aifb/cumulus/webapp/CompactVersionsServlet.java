package edu.kit.aifb.cumulus.webapp;

import edu.kit.aifb.cumulus.store.AbstractCassandraRdfHector;
import edu.kit.aifb.cumulus.store.CassandraRdfHectorFlatHash;
import edu.kit.aifb.cumulus.store.Store;
import edu.kit.aifb.cumulus.store.StoreException;
import java.io.IOException;
import java.util.logging.Logger;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.semanticweb.yars.nx.Node;

import edu.kit.aifb.cumulus.webapp.formatter.SerializationFormat;
import java.io.BufferedWriter;
import java.io.CharArrayWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import org.semanticweb.yars.nx.Variable;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;


/**
 *
 * @author tmacicas
 */
@SuppressWarnings("serial")
public class CompactVersionsServlet extends AbstractHttpServlet {
	private final Logger _log = Logger.getLogger(this.getClass().getName());

	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		long start = System.currentTimeMillis();
		ServletContext ctx = getServletContext();
		// force only text/plain for output in this case
		SerializationFormat formatter = Listener.getSerializationFormat("text/plain");
		if (formatter == null) {
			sendError(ctx, req, resp, HttpServletResponse.SC_NOT_ACCEPTABLE, "no known mime type in Accept header");
			return;
		}
		int queryLimit = (Integer)ctx.getAttribute(Listener.QUERY_LIMIT);
		resp.setCharacterEncoding("UTF-8");

		String graph = req.getParameter("graph");
		// some checks
		if( graph == null || graph.isEmpty() ) {
			sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST,
                                "Please pass the graph as parameter. ");
			return;
                }
                String encoded_graph = Store.encodeKeyspace(graph);
                CharArrayWriter buffer = new CharArrayWriter();
		BufferedWriter out = new BufferedWriter(buffer);
                AbstractCassandraRdfHector crdf = (AbstractCassandraRdfHector)ctx.getAttribute(Listener.STORE);
                if( ! crdf.existsKeyspace(encoded_graph) ) {
                    sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST,
                                "Please pass an existing graph as parameter. ");
                    return;
                }
                Hashtable<String, List<Node[]>> versioned_entities = new Hashtable<String, List<Node[]>>();
                Hashtable<String, String> previous_commit_id  = new Hashtable<String, String>();

                HashSet<String> total_entities = new HashSet<String>();
                // first of all get all entities for that graph, then the versions tree
                List<String> keys = crdf.queryAllRowKeys(encoded_graph, Integer.MAX_VALUE);
                Iterator<String> it = keys.iterator();
                // for each key of this graph, get the versioning tree
                while( it.hasNext() ) {
                    String key = it.next();
                    String exact_key = key;
                    // there are key-VER and key-URN, thus skip one of them
                    if( key.endsWith("URN>") )
                        continue;

                    total_entities.add(key);
                    // remove the brackets and the VER suffix
                    key = key.substring(1, key.lastIndexOf("-"));

                    /*// get the key entire versioning history
                    Hashtable<String, List<String>> version_history = crdf.getVersionHistory(
                            encoded_graph, key);*/
                    // get the last commit ID for this entity
                    String lastCID = crdf.lastCommitTxID(encoded_graph, key);

                    // now get the entire last version of this entity
                    Node[] n = new Node[3];
                    try {
			n[0] = getNode("<"+key+">", "s");
			n[1] = getNode(null, "p");
                        n[2] = getNode(null, "o");
                    }
                    catch (ParseException ex) {
                            _log.severe(ex.getMessage());
                            sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST,
                                    "Could not parse query string.");
                            return;
                    }
                    List<Node[]> l = new ArrayList<Node[]>(); 
                    l.add(n);
                    // get the entire entity last version
                    
// NOTE: URN IS HARDCODED

                    boolean successful_fetch = ((CassandraRdfHectorFlatHash)crdf).fetchMostRecentVersions(
                            encoded_graph, CassandraRdfHectorFlatHash.CF_S_PO, l, lastCID, "'tx_client'",
                            versioned_entities, previous_commit_id);
                    if( ! successful_fetch ) {
                            sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST,
                                    "The most recent version for key " + exact_key + " could not be fechted!");
                            return;
                    }

                    // delete the versioning data
                    String ver_metadata_key = "<"+encoded_graph+"-"+key+">";
                    crdf.deleteByRowKey(ver_metadata_key, Listener.GRAPHS_VERSIONS_KEYSPACE, 0);

                    /*
                    for(Iterator it2 = versioned_entities.keySet().iterator(); it2.hasNext(); ) {
                        System.out.println("key: " + it2.next());
                    }
                    List<Node[]> bla = versioned_entities.get(exact_key);
                    for(Iterator it2 = bla.iterator(); it2.hasNext(); ) {
                        Node[] n2 = (Node[]) it2.next();
                        System.out.println(n2[0] + " " + n2[1]);
                    }*/

                    // now delete all the versions
                    ((CassandraRdfHectorFlatHash)crdf).batchDeleteVersioning(CassandraRdfHectorFlatHash.CF_S_PO,
                            versioned_entities.get(exact_key), encoded_graph, "'tx_client'", lastCID);

                    try {
                        // now add the previously retrieved most recent entity version
                        crdf.addDataVersioning(versioned_entities.get(exact_key).iterator(), encoded_graph, 0, "'tx_client'", lastCID);
                    } catch (StoreException ex) {
                         _log.severe(ex.getMessage());
                          sendError(ctx, req, resp, HttpServletResponse.SC_BAD_REQUEST, "Could not add the most recent version back!");
                          return;
                    }
                }
                buffer.append((System.currentTimeMillis() - start)+ "ms" + "\n");
                resp.getWriter().print(buffer.toString());
		_log.info("[dataset] COMPACT stats " + (System.currentTimeMillis() - start)+ "ms; DO NOT FORGET TO RUN A cleanup using " +
                        "nodetool in order to delete all the tombstones!");
	}

	private Node getNode(String value, String varName) throws ParseException {
		if (value != null && value.trim().length() > 2)
			return NxParser.parseNode(value);
		else
			return new Variable(varName);
	}
}
