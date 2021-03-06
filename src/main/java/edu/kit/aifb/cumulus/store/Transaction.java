package edu.kit.aifb.cumulus.store;

import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.Iterator;

import java.util.logging.Logger;

public class Transaction 
{
        public enum TX_TYPES { IP, DP, UP, DE, IL, DL, SC, DC};

	private final Logger _log = Logger.getLogger(this.getClass().getName());	

	public String ID; 
        // IP -insert property, DP - delete pr, UP - update p, DE - delete entity
        // IL - insert link, DL - delete link, SC - shallow copy, DC - deep copy
        public TX_TYPES txType;
	// store all the operations that must be done here 
	public ArrayList<Operation> ops; 
	// store all the reverse operations that must be done in case of ROLLBACK
	private ArrayList<Operation> reverse_ops;

        // used only when MVCC is turned on 
        private String URN;
	
	public Transaction(String ID) { 
		this.ID = ID;
		this.ops = new ArrayList<Operation>();
		this.reverse_ops = new ArrayList<Operation>();
	}

        public Transaction(String ID, String txType) {
            this(ID);
            if( txType.equals("IP") )
                this.txType = TX_TYPES.IP;
            if( txType.equals("DP") )
                this.txType = TX_TYPES.DP;
            if( txType.equals("UP") )
                this.txType = TX_TYPES.UP;
            if( txType.equals("DE") )
                this.txType = TX_TYPES.DE;
            if( txType.equals("IL") )
                this.txType = TX_TYPES.IL;
            if( txType.equals("DL") )
                this.txType = TX_TYPES.DL;
            if( txType.equals("SC") )
                this.txType = TX_TYPES.SC;
            if( txType.equals("DC") )
                this.txType = TX_TYPES.DC;

        }

	public ArrayList<Operation> getOps() { 
		return this.ops;
	}	

	// parse the String and create a new Operation
	public int addOp(String op) { 
		Operation oper;
		// parse it 
		int indx_p1 = op.indexOf("(");
		int indx_p2 = op.lastIndexOf(")");
		if( indx_p1 == -1 || indx_p2 == -1 ) 
			return 1;
		String op_name = op.substring(0, indx_p1);
		Operation.Type op_type = Operation.map.get(op_name);
		if( op_type == null ) 
			return 2;
		oper = new Operation(op_type);
			
		// parse arguments
		String args = op.substring(indx_p1+1, indx_p2); 
		StringTokenizer st = new StringTokenizer(args, ",");
		int counter = -1;
		while( st.hasMoreTokens() ) { 
			oper.addParam(st.nextToken(), ++counter);	
		}
		// check if all parameters are sent
		if( Operation.needed_params.get(op_type) != ++counter )
			return 3;
		// add this operation part of the transaction
                registerOperation(oper);
		
                // add the reversed op for being able to Rollback
                registerReverseOperation(oper);
		return 0;
	}

        private void registerOperation(Operation oper) {
            this.ops.add(oper);

            // however, if update, then add a LOCK operation for locking the
            //entity that will be deleted
            if( oper.getType() == Operation.Type.UPDATE ) {
                Operation lock_op = new Operation(oper.getType());
                lock_op.just_for_locking = true;
                lock_op.copyParam(oper);
                lock_op.swapOldNewValuesParam();
                this.ops.add(lock_op);
            }
            // if clone, then add another LOCK operation for the entire entity
            else if( oper.getType() == Operation.Type.ENT_SHALLOW_CLONE ||
                oper.getType() == Operation.Type.ENT_DEEP_CLONE ) {
                Operation lock_op = new Operation(oper.getType());
                lock_op.just_for_locking = true;
                lock_op.copyParam(oper);
                lock_op.swapEwithV();
                this.ops.add(lock_op);
            }

            // if linking operation
            if( oper.getType() == Operation.Type.INSERT_LINK ||
                  oper.getType() == Operation.Type.UPDATE_LINK ||
                  oper.getType() == Operation.Type.DELETE_LINK ) {
                Operation lock_op = new Operation(oper.getType());
                lock_op.just_for_locking = true;
                lock_op.copyParam(oper);
                if( oper.getType() == Operation.Type.UPDATE ) {
                    lock_op.swapOldNewValuesParam();
                }
                lock_op.prepareForLinks();
                this.ops.add(lock_op);

                // however, if update, then add a LOCK operation for locking the
                //entity that will be deleted
                if( oper.getType() == Operation.Type.UPDATE ) {
                    lock_op = new Operation(oper.getType());
                    lock_op.just_for_locking = true;
                    lock_op.copyParam(oper);
                    lock_op.prepareForLinks();
                    this.ops.add(lock_op);
                }
            }
        }
        
        private void registerReverseOperation(Operation oper) {
            this.addReverseOp(oper);
            // no need to add extra LOCK operation as we did for registerOperation()
            //method, as when this is run the locks are already acquired
        }

	private void addReverseOp(Operation op) { 
		//based on what kind of op is
		Operation oper = new Operation(Operation.reverse.get(op.getType()));
	  	oper.copyParam(op);
		// invert v_old with v_new if was an update 
		if( oper.getType() == Operation.Type.UPDATE ) { 
			oper.swapOldNewValuesParam();
		}
		// now add it to the ArrayList 
		this.reverse_ops.add(oper);
	}

	public Operation getReverseOp(int index) { 
		return this.reverse_ops.get(index);
	}

        public String getURN() {
            return this.URN;
        }

        public void setURN(String URN) {
            this.URN = URN;
        }

	public void printTransaction() { 
		StringBuffer buf = new StringBuffer("Transaction ID: " + this.ID); 
		buf.append("\n");
		for( Iterator it = ops.iterator(); it.hasNext(); ) { 
			Operation o =  (Operation)it.next(); 
		 	buf.append(o.printToString());	
		}	
		_log.info(buf.toString());
		return;
	}
}
