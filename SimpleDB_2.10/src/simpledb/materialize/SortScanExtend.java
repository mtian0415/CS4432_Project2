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
		this.tx = tx;
		this.originalTable.beforeFirst();
	}

	public boolean next() {
		if (this.tableInfo.getIsSorted()) {
			System.out.println("Table " + this.tableInfo.fileName()
					+ " is sorted already");
			return this.originalTable.next();
		} else {
			System.out.println("Table " + this.tableInfo.fileName()
					+ " is NOT sorted");
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
		return this.originalTable.getVal(fieldName);
	}

	public int getInt(String fieldName) {
		return this.originalTable.getInt(fieldName);
	}

	public String getString(String fieldName) {
		return this.originalTable.getString(fieldName);
	}

	public boolean hasField(String fieldName) {
		return this.originalTable.hasField(fieldName);
	}
}
