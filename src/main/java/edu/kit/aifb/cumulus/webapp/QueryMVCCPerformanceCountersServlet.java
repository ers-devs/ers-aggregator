package edu.kit.aifb.cumulus.webapp;

import edu.kit.aifb.cumulus.store.CassandraRdfHectorFlatHash;
import edu.kit.aifb.cumulus.store.ExecuteTransactions;
import java.io.IOException;
import java.util.logging.Logger;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.semanticweb.yars.nx.Node;

import edu.kit.aifb.cumulus.webapp.formatter.SerializationFormat;
import org.semanticweb.yars.nx.Variable;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;


/**
 *
 * @author tmacicas
 */
@SuppressWarnings("serial")
public class QueryMVCCPerformanceCountersServlet extends AbstractHttpServlet {
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

                StringBuffer sb = new StringBuffer();
                sb.append("Add to 'not-yet-c', Remove from 'not-yet-c', Insert, Update \n");
                sb.append((ExecuteTransactions.add_pending_tx.get()+0.0)/ExecuteTransactions.run_tx.get()).append(", ");
                sb.append((ExecuteTransactions.remove_pending_tx.get()+0.0)/ExecuteTransactions.run_tx.get()).append(", ");
                sb.append((ExecuteTransactions.add_data_versioning.get()+0.0)/ExecuteTransactions.run_tx.get()).append(", ");
                sb.append((ExecuteTransactions.update_data_versioning.get()+0.0)/ExecuteTransactions.run_tx.get()).append("\n");

                sb.append("Update \n");
                sb.append("1.Get pending tx CID, 2.Query all prev CID, 3. Process all prev CIDs,");
                sb.append("4.Get last commit ID (1+2+3+.), 5.Fetch most recent version (1+2+3+4+.), ");
                sb.append("Process versions, Mutations, Commit or abort \n");
                sb.append((CassandraRdfHectorFlatHash.get_pending_tx.get()+0.0)/CassandraRdfHectorFlatHash.no_get_pending_tx.get()).append(", ");
                sb.append((CassandraRdfHectorFlatHash.query_all_prev_cid.get()+0.0)/CassandraRdfHectorFlatHash.no_query_all_prev_cid.get()).append(", ");
                sb.append((CassandraRdfHectorFlatHash.process_all_prev_cid.get()+0.0)/CassandraRdfHectorFlatHash.no_process_all_prev_cid.get()).append(", ");
                sb.append((CassandraRdfHectorFlatHash.last_commit_id.get()+0.0)/CassandraRdfHectorFlatHash.no_last_commit_id.get()).append(", ");
                sb.append((CassandraRdfHectorFlatHash.fetch_most_recent_v.get()+0.0)/CassandraRdfHectorFlatHash.no_fetch_most_recent_v.get()).append(", ");
                sb.append((CassandraRdfHectorFlatHash.process_versions.get()+0.0)/CassandraRdfHectorFlatHash.no_process_versions.get()).append(", ");
                sb.append((CassandraRdfHectorFlatHash.mutation_version.get()+0.0)/CassandraRdfHectorFlatHash.no_mutation_version.get()).append(", ");
                sb.append((CassandraRdfHectorFlatHash.commit_abort.get()+0.0)/CassandraRdfHectorFlatHash.no_commit_abort.get()).append("\n");

                // now reset the counters if reset is given
                String r = req.getParameter("reset");
                if( r != null ) {
                    ExecuteTransactions.add_pending_tx.set(0L);
                    ExecuteTransactions.remove_pending_tx.set(0L);
                    ExecuteTransactions.add_data_versioning.set(0L);
                    ExecuteTransactions.update_data_versioning.set(0L);
                    ExecuteTransactions.run_tx.set(0);

                    CassandraRdfHectorFlatHash.get_pending_tx.set(0L);
                    CassandraRdfHectorFlatHash.no_get_pending_tx.set(0);
                    CassandraRdfHectorFlatHash.query_all_prev_cid.set(0L);
                    CassandraRdfHectorFlatHash.no_query_all_prev_cid.set(0);
                    CassandraRdfHectorFlatHash.process_all_prev_cid.set(0L);
                    CassandraRdfHectorFlatHash.no_process_all_prev_cid.set(0);
                    CassandraRdfHectorFlatHash.last_commit_id.set(0L);
                    CassandraRdfHectorFlatHash.no_last_commit_id.set(0);
                    CassandraRdfHectorFlatHash.fetch_most_recent_v.set(0L);
                    CassandraRdfHectorFlatHash.no_fetch_most_recent_v.set(0);
                    CassandraRdfHectorFlatHash.process_versions.set(0L);
                    CassandraRdfHectorFlatHash.no_process_versions.set(0);
                    CassandraRdfHectorFlatHash.mutation_version.set(0L);
                    CassandraRdfHectorFlatHash.no_mutation_version.set(0);
                    CassandraRdfHectorFlatHash.commit_abort.set(0L);
                    CassandraRdfHectorFlatHash.no_commit_abort.set(0);
                }

                resp.getWriter().println(sb.toString());
		_log.info("[dataset] QUERY versions performance counters " +
                        (System.currentTimeMillis() - start));
	}

	private Node getNode(String value, String varName) throws ParseException {
		if (value != null && value.trim().length() > 2)
			return NxParser.parseNode(value);
		else
			return new Variable(varName);
	}
}
