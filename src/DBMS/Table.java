package DBMS;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;

@SuppressWarnings("unused")
public class Table implements Serializable {
	private String name;
	private String[] columnsNames;
	private int pageCount;
	private int recordsCount;
	private ArrayList<String> trace;
	private ArrayList<String> IndexIndices;

	public Table(String name, String[] columnsNames) {
		super();
		this.name = name;
		this.columnsNames = columnsNames;
		this.trace = new ArrayList<String>();
		this.trace.add("Table created name:" + name + ", columnsNames:"
				+ Arrays.toString(columnsNames));
		this.IndexIndices = new ArrayList<String>();
	}

	@Override
	public String toString() {
		return "Table [name=" + name + ", columnsNames="
				+ Arrays.toString(columnsNames) + ", pageCount=" + pageCount
				+ ", recordsCount=" + recordsCount + "]";
	}

	public void insert(String[] record) {
		long startTime = System.currentTimeMillis();
		Page current = FileManager.loadTablePage(this.name, pageCount - 1);
		if (current == null || !current.insert(record)) {
			current = new Page();
			current.insert(record);
			pageCount++;
		}
		FileManager.storeTablePage(this.name, pageCount - 1, current);
		recordsCount++;
		long stopTime = System.currentTimeMillis();
		this.trace.add("Inserted:" + Arrays.toString(record) + ", at page number:" + (pageCount - 1)
				+ ", execution time (mil):" + (stopTime - startTime));
	}

	public String[] fixCond(String[] cols, String[] vals) {
		String[] res = new String[columnsNames.length];
		for (int i = 0; i < res.length; i++) {
			for (int j = 0; j < cols.length; j++) {
				if (columnsNames[i].equals(cols[j])) {
					res[i] = vals[j];
				}
			}
		}
		return res;
	}

	public ArrayList<String[]> select(String[] cols, String[] vals) {
		String[] cond = fixCond(cols, vals);
		String tracer = "Select condition:" + Arrays.toString(cols) + "->" + Arrays.toString(vals);
		ArrayList<ArrayList<Integer>> pagesResCount = new ArrayList<ArrayList<Integer>>();
		ArrayList<String[]> res = new ArrayList<String[]>();
		long startTime = System.currentTimeMillis();
		for (int i = 0; i < pageCount; i++) {
			Page p = FileManager.loadTablePage(this.name, i);
			ArrayList<String[]> pRes = p.select(cond);
			if (pRes.size() > 0) {
				ArrayList<Integer> pr = new ArrayList<Integer>();
				pr.add(i);
				pr.add(pRes.size());
				pagesResCount.add(pr);
				res.addAll(pRes);
			}
		}
		long stopTime = System.currentTimeMillis();
		tracer += ", Records per page:" + pagesResCount + ", records:" + res.size()
				+ ", execution time (mil):" + (stopTime - startTime);
		this.trace.add(tracer);
		return res;
	}

	public ArrayList<String[]> select(int pageNumber, int recordNumber) {
		String tracer = "Select pointer page:" + pageNumber + ", record:" + recordNumber;
		ArrayList<String[]> res = new ArrayList<String[]>();
		long startTime = System.currentTimeMillis();
		Page p = FileManager.loadTablePage(this.name, pageNumber);
		ArrayList<String[]> pRes = p.select(recordNumber);
		if (pRes.size() > 0) {
			res.addAll(pRes);
		}
		long stopTime = System.currentTimeMillis();
		tracer += ", total output count:" + res.size()
				+ ", execution time (mil):" + (stopTime - startTime);
		this.trace.add(tracer);
		return res;
	}

	public ArrayList<String[]> select() {
		ArrayList<String[]> res = new ArrayList<String[]>();
		long startTime = System.currentTimeMillis();
		for (int i = 0; i < pageCount; i++) {
			Page p = FileManager.loadTablePage(this.name, i);
			res.addAll(p.select());
		}
		long stopTime = System.currentTimeMillis();
		this.trace.add("Select all pages:" + pageCount + ", records:" + recordsCount
				+ ", execution time (mil):" + (stopTime - startTime));
		return res;
	}

	public String getFullTrace() {
		String res = "";
		for (int i = 0; i < this.trace.size(); i++) {
			res += this.trace.get(i) + "\n";
		}
		return res + "Pages Count: " + pageCount + ", Records Count: " + recordsCount;
	}

	public String getLastTrace() {
		return this.trace.get(this.trace.size() - 1);
	}

	public ArrayList<String[]> missingRecords(ArrayList<String[]> records) {
		ArrayList<String[]> missing = new ArrayList<>();
		ArrayList<String> fulltrace = trace;
		ArrayList<String[]> allRecords = new ArrayList<>();
		for (String record : fulltrace) {
			if (record.startsWith("Inserted:")) {
				int startIndex = record.indexOf("[");
				int endIndex = record.indexOf("]");
				String recordString = record.substring(startIndex + 1, endIndex);
				String[] recordArray = recordString.split(", ");
				allRecords.add(recordArray);
			}
		}
		for (String[] record : allRecords) {
			boolean found = false;
			for (String[] rec : records) {
				if (Arrays.equals(record, rec)) {
					found = true;
					break;
				}
			}
			if (!found) {
				missing.add(record);
			}
		}

		return missing;

	}

	public void recoverRecords(ArrayList<String[]> missing) {
		ArrayList<String> allRecords = trace;
		for (String record : allRecords) {
			if (record.startsWith("Inserted:")) {
				int startIndex = record.indexOf("[");
				int endIndex = record.indexOf("]");
				String recordString = record.substring(startIndex + 1, endIndex);
				String[] recordArray = recordString.split(", ");
				for (String[] rec : missing) {
					if (Arrays.equals(recordArray, rec)) {
						int number = getPageNumber(record);
						Page p = FileManager.loadTablePage(this.name, number);
						if (p != null) {
							p.insert(recordArray);
							FileManager.storeTablePage(this.name, number, p);
						} else {
							Page newPage = new Page();
							newPage.insert(recordArray);
							FileManager.storeTablePage(this.name, number, newPage);
						}
					}
				}
			}
		}
	}

	private int getPageNumber(String recordString) {
		String marker = "at page number:";
		int startIndex = recordString.indexOf(marker);
		if (startIndex == -1) {
			throw new IllegalArgumentException("Could not find 'at page number:' in: " + recordString);
		}

		startIndex += marker.length();

		// Read until next comma or end of line
		int endIndex = recordString.indexOf(',', startIndex);
		if (endIndex == -1)
			endIndex = recordString.length();

		String numberStr = recordString.substring(startIndex, endIndex).trim();
		return Integer.parseInt(numberStr); // should return 0 for your example
	}

	public int getColumnIndex(String columnName) {
		for (int i = 0; i < columnsNames.length; i++) {
			if (columnsNames[i].equals(columnName)) {
				return i;
			}
		}
		return -1;
	}

	public int getIndexNumber(String[] cols) {
		int count = 0;
		for (String column : cols) {
			for (int j = 0; j < columnsNames.length; j++) {
				if (column == columnsNames[j]) {
					count++;
					break;
				}
			}
		}
		return 0;
	}

	public void setIndexNumber(String columnName) {
		for (int i = 0; i < columnsNames.length; i++) {
			if (columnsNames[i].equals(columnName)) {
				IndexIndices.add(columnsNames[i]);
				return;
			}
		}
	}

	public ArrayList<String> getIndexIndices() {
		return IndexIndices;
	}

	public ArrayList<String[]> tableRecords() {
		ArrayList<String[]> res = new ArrayList<String[]>();
		for (int i = 0; i < pageCount; i++) {
			Page p = FileManager.loadTablePage(this.name, i);
			if (p != null)
				res.addAll(p.select());
		}
		return res;
	}

	public ArrayList<String[]> getRecordsWithCondition(String[] cols, String[] vals) {
		String[] cond = fixCond(cols, vals);
		ArrayList<ArrayList<Integer>> pagesResCount = new ArrayList<ArrayList<Integer>>();
		ArrayList<String[]> res = new ArrayList<String[]>();
		for (int i = 0; i < pageCount; i++) {
			Page p = FileManager.loadTablePage(this.name, i);
			ArrayList<String[]> pRes = p.select(cond);
			if (pRes.size() > 0) {
				ArrayList<Integer> pr = new ArrayList<Integer>();
				pr.add(i);
				pr.add(pRes.size());
				pagesResCount.add(pr);
				res.addAll(pRes);
			}
		}
		return res;
	}

	public int getColumnLength() {
		return columnsNames.length;
	}
}
