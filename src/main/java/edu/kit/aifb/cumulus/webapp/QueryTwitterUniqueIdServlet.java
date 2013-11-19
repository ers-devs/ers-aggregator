package edu.kit.aifb.cumulus.webapp;

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
public class QueryTwitterUniqueIdServlet extends AbstractHttpServlet {
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

		String trials = req.getParameter("trials");
                int no_trials = 10000;  // default value
		if( trials != null && ! trials.isEmpty() ) {
                    no_trials = Integer.valueOf(trials);
                }

                long start_time = System.currentTimeMillis();
                for( int i=0; i<no_trials; ++i ) {
                     String id = Listener.SNOWFLAKE_GENERATOR.getStringId();
                }
                long total_time = System.currentTimeMillis()-start_time;

		String msg = "OK " + req.getRequestURI() + " " +
                        String.valueOf(HttpServletResponse.SC_OK) + "\n";
                msg += " total time, no trials, time per trial \n";
                msg += total_time + "," + no_trials + "," + (total_time/no_trials);
                resp.getWriter().println(msg);
		_log.info("[dataset] QUERY Twitter ID ");
	}

	private Node getNode(String value, String varName) throws ParseException {
		if (value != null && value.trim().length() > 2)
			return NxParser.parseNode(value);
		else
			return new Variable(varName);
	}
}
