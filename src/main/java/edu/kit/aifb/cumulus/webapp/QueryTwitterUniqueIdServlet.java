package edu.kit.aifb.cumulus.webapp;

import java.io.IOException;
import java.util.logging.Logger;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


import edu.kit.aifb.cumulus.webapp.formatter.SerializationFormat;

/**
 *
 * @author tmacicas
 */
@SuppressWarnings("serial")
public class QueryTwitterUniqueIdServlet extends AbstractHttpServlet {
	private final Logger _log = Logger.getLogger(this.getClass().getName());

        @Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		long start = System.currentTimeMillis();
		ServletContext ctx = getServletContext();
		// force only text/plain for output in this case
		SerializationFormat formatter = Listener.getSerializationFormat("text/plain");
		if (formatter == null) {
			sendError(ctx, req, resp, HttpServletResponse.SC_NOT_ACCEPTABLE, "no known mime type in Accept header");
			return;
		}
		resp.setCharacterEncoding("UTF-8");

                _log.info("TEST1");

		String trials = req.getParameter("trials");
                int no_trials = 10000;  // default value
		if( trials != null && ! trials.isEmpty() ) {
                    no_trials = Integer.valueOf(trials);
                }

                _log.info("TEST2");

                long start_time = System.currentTimeMillis();
                for( int i=0; i<no_trials; ++i ) {
                     String id = Listener.SNOWFLAKE_GENERATOR.getStringId();
                }
                long total_time = System.currentTimeMillis()-start_time;

                _log.info("TEST3");

		String msg = "OK " + req.getRequestURI() + " " +
                        String.valueOf(HttpServletResponse.SC_OK) + "\n";
                msg += " total time, no trials, time per trial \n";
                msg += total_time + "," + no_trials + "," + ((total_time+0.0f)/no_trials);
                resp.getWriter().println(msg);

                _log.info("TEST4");

		_log.info("[dataset] QUERY Twitter ID ");
	}
}
