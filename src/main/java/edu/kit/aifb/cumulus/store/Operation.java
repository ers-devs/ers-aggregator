package edu.kit.aifb.cumulus.store; 

import edu.kit.aifb.cumulus.webapp.Listener;
import java.util.Hashtable;

public class Operation 
{
	public static enum Type {
                // clasical operations
		GET, INSERT, UPDATE, DELETE,
                // clone and delete an entire entity
                ENT_SHALLOW_CLONE, ENT_DEEP_CLONE, ENT_DELETE,
                // generic used just for locking
                LOCK
	}

	public static final String GET_OP_NAME = "get"; 
	public static final String INSERT_OP_NAME = "insert";
	public static final String UPDATE_OP_NAME = "update"; 
	public static final String DELETE_OP_NAME = "delete";
        // if links is set to on, then the links must be also managed (either 
        //created, updated or deleted); for this reason, another generic 
        //operation must be added to the list just to acquire the locks
        public static final String JUST_LOCK_OP_NAME = "lock";
        public static final String SHALLOW_CLONE_OP_NAME = "shallow_clone";
        public static final String DEEP_CLONE_OP_NAME = "deep_clone";

        public static final String DELETE_ALL_OP_NAME = "delete_all";

	public static Hashtable<String, Type> map = new Hashtable<String,Type>();
	public static Hashtable<Type, Type> reverse = new Hashtable<Type, Type>();
	
        // set this to true if this operation is used only in the locking logic
        public boolean just_for_locking = false;

	static { 
		map.put(GET_OP_NAME, Type.GET);
		map.put(INSERT_OP_NAME, Type.INSERT);
		map.put(UPDATE_OP_NAME, Type.UPDATE);
		map.put(DELETE_OP_NAME, Type.DELETE);
                map.put(JUST_LOCK_OP_NAME, Type.LOCK);
                map.put(SHALLOW_CLONE_OP_NAME, Type.ENT_SHALLOW_CLONE);
                map.put(DEEP_CLONE_OP_NAME, Type.ENT_DEEP_CLONE);
                map.put(DELETE_ALL_OP_NAME, Type.ENT_DELETE);

		reverse.put(Type.GET, Type.GET); 
		reverse.put(Type.INSERT, Type.DELETE); 
		reverse.put(Type.DELETE, Type.INSERT); 
		reverse.put(Type.UPDATE, Type.UPDATE);
                reverse.put(Type.ENT_SHALLOW_CLONE, Type.ENT_DELETE);
                reverse.put(Type.ENT_DEEP_CLONE, Type.ENT_DELETE);
                reverse.put(Type.ENT_DELETE, Type.ENT_DEEP_CLONE);
                reverse.put(Type.LOCK, Type.LOCK);
	}
	
	/* a simplistic pattern: 
 		op_type(e, p, v, g, [v_old])\r\n
	*/
	private Type type; 
	// e, p, v, g, v_old as params
	public String[] params = new String[5];

	public Operation() { 
		init();
	}

	public Operation(Type type) { 
		this();
		this.type = type; 
	}

	private void init() { 
		for( int i=0; i<5; ++i ) { 
			params[i] = new String();
		}
	}
		
	public void addParam(String param, int pos) { 
		params[pos] = new String(param.trim());
	}
	
	public String getParam(int pos) {
		return params[pos];
	}

	public void copyParam(Operation op) { 
		for( int i=0; i<params.length; ++i ) { 
			this.addParam(op.getParam(i), i);
		}
	}

	public Type getType() { 
		return this.type;
	}

	public void swapOldNewValuesParam() {
		String tmp = params[2]; // v_new
		params[2] = params[4]; 	// v_new = v_old
		params[4] = params[2]; 	// v_old = v_old
	}

        public void prepareForLinks() {
            // swap v with e
            String tmp = params[2];
            params[2] = params[0];
            params[0] = tmp;
            tmp = params[1];
            // change P, add the Listener prefix
            if( params[1].startsWith("<") )
                params[1] = "<";
            else if( params[1].startsWith("\"") ) 
                params[1] = "\"";
            params[1] += Listener.DEFAULT_ERS_LINKS_PREFIX + tmp.substring(1);
        }

        public void swapEwithV() {
            // swap v with e
            String tmp = params[2];
            params[2] = params[0];
            params[0] = tmp;
        }

	public String printToString() { 
		StringBuffer buf = new StringBuffer("Type " + this.type);
                if( just_for_locking )
                    buf.append(" - used just for locking - ");
		buf.append(" arguments: (");
		for( int i=0; i<5; ++i ) { 
			buf.append(params[i]);
			if( i != 4 ) 
				buf.append(",");
		}
		buf.append(")\n");
		return buf.toString();
	}

}
