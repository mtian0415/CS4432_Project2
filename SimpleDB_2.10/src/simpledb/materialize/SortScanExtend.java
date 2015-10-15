package simpledb.materialize;

import java.util.List;

import simpledb.query.Constant;
import simpledb.query.TablePlan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.remote.SimpleConnection;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/* CS4432-Project2(Task4,5): new class extends SortScan by Mi Tian, Yuchen Liu 
 * This class extends the original SortScan class and overwrites all of it functions
 * so that if the table is already sorted, it will use it directly instead of sorting them again
 * */
public class SortScanExtend extends SortScan {
	private TableInfo tableInfo;
	private Schema schema;
	private Transaction tx;
	private UpdateScan originalTable;

	public SortScanExtend(List<TempTable> runs, RecordComparator comp,
			TablePlan p, Transaction tx) {
		super(runs, comp);
		this.originalTable = (UpdateScan) p.open();
		this.tableInfo = p.getTableInfo();
		this.schema = this.tableInfo.schema();
		this.tx = tx;
		this.originalTable.beforeFirst();
		System.out.println("SortScanExtended is "
				+ (this.tableInfo.getIsSorted() ? "Sorted" : "Not Sorted"));
	}

	public void beforeFirst() {
		if (this.tableInfo.getIsSorted()) {
			this.originalTable.beforeFirst();
		} else {
			super.beforeFirst();
		}
	}

	public boolean next() {
		if (this.tableInfo.getIsSorted()) {
			return this.originalTable.next();
		} else {
			boolean next = super.next();

			// copy sorted table into original table
			if (next && this.originalTable.next()) {
				for (String fieldName : this.schema.fields()) {
					this.originalTable.setVal(fieldName,
							super.getVal(fieldName));
				}
			} else {
				this.tableInfo.setIsSorted(true);
				SimpleDB.mdMgr().updateSortedTable(this.tableInfo, tx);
			}

			return next;
		}
	}

	public void close() {
		super.close();
		this.originalTable.close();
	}

	public Constant getVal(String fieldName) {
		if (this.tableInfo.getIsSorted()) {
			return this.originalTable.getVal(fieldName);
		} else {
			return super.getVal(fieldName);
		}
	}

	public int getInt(String fieldName) {
		if (this.tableInfo.getIsSorted()) {
			return this.originalTable.getInt(fieldName);
		} else {
			return super.getInt(fieldName);
		}
	}

	public String getString(String fieldName) {
		if (this.tableInfo.getIsSorted()) {
			return this.originalTable.getString(fieldName);
		} else {
			return super.getString(fieldName);
		}
	}

	public boolean hasField(String fieldName) {
		if (this.tableInfo.getIsSorted()) {
			return this.originalTable.hasField(fieldName);
		} else {
			return super.hasField(fieldName);
		}
	}
	
	public void savePosition() {
		if(!this.tableInfo.getIsSorted()) {
			super.savePosition();
		}
	}
	
	public void restorePosition() {
		if(!this.tableInfo.getIsSorted()) {
			super.restorePosition();
		}
	}
}
