package DBMS;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import javafx.scene.control.Tab;

@SuppressWarnings("unused")
public class DBApp {
	static int dataPageSize = 2;

	public static void createTable(String tableName, String[] columnsNames) {
		Table t = new Table(tableName, columnsNames);
		FileManager.storeTable(tableName, t);
	}

	public static void insert(String tableName, String[] record) {
		Table t = FileManager.loadTable(tableName);
		t.insert(record);
		FileManager.storeTable(tableName, t);
		ArrayList<String> indices = t.getIndexIndices();
		for (String index : indices) {
			BitmapIndex b = FileManager.loadTableIndex(tableName, index);
			b.updateTable(t);
			FileManager.storeTableIndex(tableName, index, b);
		}
	}

	public static ArrayList<String[]> select(String tableName) {
		Table t = FileManager.loadTable(tableName);
		ArrayList<String[]> res = t.select();
		FileManager.storeTable(tableName, t);
		return res;
	}

	public static ArrayList<String[]> select(String tableName, int pageNumber, int recordNumber) {
		Table t = FileManager.loadTable(tableName);
		ArrayList<String[]> res = t.select(pageNumber, recordNumber);
		FileManager.storeTable(tableName, t);
		return res;
	}

	public static ArrayList<String[]> select(String tableName, String[] cols, String[] vals) {
		Table t = FileManager.loadTable(tableName);
		ArrayList<String[]> res = t.select(cols, vals);
		FileManager.storeTable(tableName, t);
		return res;
	}

	public static String getFullTrace(String tableName) {
		Table t = FileManager.loadTable(tableName);
		String res = t.getFullTrace();
		return res;
	}

	public static String getLastTrace(String tableName) {
		Table t = FileManager.loadTable(tableName);
		String res = t.getLastTrace();
		return res;
	}

	public static ArrayList<String[]> validateRecords(String tableName) {
		Table t = FileManager.loadTable(tableName);
		ArrayList<String[]> res = t.tableRecords();
		ArrayList<String[]> result = t.missingRecords(res);
		t.updateTrace("Validating records: " + result.size() + " records missing.");
		FileManager.storeTable(tableName, t);
		return result;
	}

	public static void recoverRecords(String tableName, ArrayList<String[]> missing) {
		Table t = FileManager.loadTable(tableName);
		String s = t.recoverRecords(missing);
		t.updateTrace("Recovering " + missing.size() + " records in pages: " + s + ".");
		FileManager.storeTable(tableName, t);
	}

	public static void createBitMapIndex(String tableName, String colName) {
		long start = System.currentTimeMillis();
		Table t = FileManager.loadTable(tableName);
		if (t == null) {
			System.out.println("Table not found");
			return;
		}
		BitmapIndex b = new BitmapIndex(t, colName);
		t.setIndexNumber(colName);
		t.updateTrace("Index created for column: " + colName + ", execution time(mil):"
				+ (System.currentTimeMillis() - start));
		FileManager.storeTable(tableName, t);
		FileManager.storeTableIndex(tableName, colName, b);
	}

	public static String getValueBits(String tableName, String colName, String value) {
		BitmapIndex b = FileManager.loadTableIndex(tableName, colName);
		String res = b.getValueBits(value);

		return res;
	}

	public static ArrayList<String[]> selectIndex(String tableName, String[] cols, String[] vals) {
		long start = System.currentTimeMillis();
		Table t = FileManager.loadTable(tableName);
		ArrayList<String[]> result = new ArrayList<>();

		// Sort the input columns and corresponding values lex order together
		sortColsAndVals(cols, vals);

		int IndexSize = t.getIndexNumber(cols);

		if (IndexSize == cols.length) { // all columns are indexed
			String bits = "";
			for (int i = 0; i < cols.length; i++) {
				BitmapIndex b = FileManager.loadTableIndex(tableName, cols[i]);
				String x = b.getValueBits(vals[i]);
				if (bits.length() == 0)
					bits = x;
				else
					bits = bitWiseAnd(bits, x);
			}
			result = getRecords(bits, t);

			// The indexed columns here are all the condition columns (sorted already)
			t.updateTrace("Select index condition:" + Arrays.toString(cols) + "->" + Arrays.toString(vals)
					+ ", Indexed columns: " + Arrays.toString(cols)
					+ ", Indexed selection count: " + result.size()
					+ ", Final count: " + result.size()
					+ ", execution time (mil):" + (System.currentTimeMillis() - start));
			FileManager.storeTable(tableName, t);

		} else if (IndexSize == 1) { // only one index
			ArrayList<String> indexed = new ArrayList<>();
			String indexedColumn = null;

			// Find which one is indexed
			ArrayList<String> tableIndexedCols = t.getIndexIndices();
			for (String c : cols) {
				if (tableIndexedCols.contains(c)) {
					indexedColumn = c;
					break;
				}
			}
			if (indexedColumn == null) {
				// no indexed column found, fallback to no index case
				result = t.selectIndex(cols, vals);
				t.updateTrace("Select index condition:" + Arrays.toString(cols) + "->" + Arrays.toString(vals)
						+ ", Non Indexed: " + Arrays.toString(cols)
						+ ", Final count: " + result.size() + ", execution time (mil):"
						+ (System.currentTimeMillis() - start));
				FileManager.storeTable(tableName, t);
				return result;
			}
			indexed.add(indexedColumn);

			BitmapIndex b = FileManager.loadTableIndex(tableName, indexedColumn);

			// Get the value for the indexed column
			int idxVal = -1;
			for (int i = 0; i < cols.length; i++) {
				if (cols[i].equals(indexedColumn)) {
					idxVal = i;
					break;
				}
			}
			String bitString = b.getValueBits(vals[idxVal]);

			// Prepare non-indexed columns and values arrays
			ArrayList<String> nonIndexedColsList = new ArrayList<>();
			ArrayList<String> nonIndexedValsList = new ArrayList<>();
			for (int i = 0; i < cols.length; i++) {
				if (!cols[i].equals(indexedColumn)) {
					nonIndexedColsList.add(cols[i]);
					nonIndexedValsList.add(vals[i]);
				}
			}

			// Sort non-indexed columns and values together before use
			String[] nonIndexedCols = nonIndexedColsList.toArray(new String[0]);
			String[] nonIndexedVals = nonIndexedValsList.toArray(new String[0]);
			sortColsAndVals(nonIndexedCols, nonIndexedVals);

			ArrayList<String[]> recordsFromIndex = getRecords(bitString, t);
			ArrayList<String[]> recordsWithConditions = t.getRecordsWithCondition(nonIndexedCols, nonIndexedVals);
			result = AndingRecords(recordsFromIndex, recordsWithConditions);

			t.updateTrace("Select index condition:" + Arrays.toString(cols) + "->" + Arrays.toString(vals)
					+ ", Indexed columns: " + Arrays.toString(indexed.toArray(new String[0]))
					+ ", Indexed selection count: " + recordsFromIndex.size()
					+ ", Non Indexed: " + Arrays.toString(nonIndexedCols)
					+ ", Final count: " + result.size()
					+ ", execution time (mil):" + (System.currentTimeMillis() - start));
			FileManager.storeTable(tableName, t);

		} else if (IndexSize == 0) { // no index
			result = t.selectIndex(cols, vals);
			// cols and vals already sorted at start
			t.updateTrace("Select index condition:" + Arrays.toString(cols) + "->" + Arrays.toString(vals)
					+ ", Non Indexed: " + Arrays.toString(cols)
					+ ", Final count: " + result.size()
					+ ", execution time (mil):" + (System.currentTimeMillis() - start));
			FileManager.storeTable(tableName, t);

		} else { // some columns are indexed
			ArrayList<String> tableIndexedCols = t.getIndexIndices();
			ArrayList<String> indexed = new ArrayList<>();
			ArrayList<String> indexedVals = new ArrayList<>();

			// Collect indexed columns and their corresponding values
			for (int i = 0; i < cols.length; i++) {
				if (tableIndexedCols.contains(cols[i])) {
					indexed.add(cols[i]);
					indexedVals.add(vals[i]);
				}
			}

			// Sort indexed columns and values together
			String[] indexedArr = indexed.toArray(new String[0]);
			String[] indexedValsArr = indexedVals.toArray(new String[0]);
			sortColsAndVals(indexedArr, indexedValsArr);

			// Compute bits intersection from all indexed conditions
			String bits = "";
			for (int i = 0; i < indexedArr.length; i++) {
				BitmapIndex b = FileManager.loadTableIndex(tableName, indexedArr[i]);
				String x = b.getValueBits(indexedValsArr[i]);
				if (i == 0)
					bits = x;
				else
					bits = bitWiseAnd(bits, x);
			}

			// Prepare non-indexed columns and values
			ArrayList<String> nonIndexedColsList = new ArrayList<>();
			ArrayList<String> nonIndexedValsList = new ArrayList<>();
			for (int i = 0; i < cols.length; i++) {
				if (!indexed.contains(cols[i])) {
					nonIndexedColsList.add(cols[i]);
					nonIndexedValsList.add(vals[i]);
				}
			}

			String[] nonIndexedCols = nonIndexedColsList.toArray(new String[0]);
			String[] nonIndexedVals = nonIndexedValsList.toArray(new String[0]);
			sortColsAndVals(nonIndexedCols, nonIndexedVals);

			ArrayList<String[]> recordsFromIndex = getRecords(bits, t);
			ArrayList<String[]> recordsWithConditions = t.getRecordsWithCondition(nonIndexedCols, nonIndexedVals);
			result = AndingRecords(recordsFromIndex, recordsWithConditions);

			t.updateTrace("Select index condition:" + Arrays.toString(cols) + "->" + Arrays.toString(vals)
					+ ", Indexed columns: " + Arrays.toString(indexedArr)
					+ ", Indexed selection count: " + recordsFromIndex.size()
					+ ", Non Indexed: " + Arrays.toString(nonIndexedCols)
					+ ", Final count: " + result.size()
					+ ", execution time (mil):" + (System.currentTimeMillis() - start));
			FileManager.storeTable(tableName, t);
		}

		return result;
	}

	// Helper class for paired sorting
	private static class ColValPair {
		String col;
		String val;

		ColValPair(String c, String v) {
			col = c;
			val = v;
		}
	}

	// Sort columns and values lexicographically together
	private static void sortColsAndVals(String[] cols, String[] vals) {
		ArrayList<ColValPair> pairs = new ArrayList<>();
		for (int i = 0; i < cols.length; i++) {
			pairs.add(new ColValPair(cols[i], vals[i]));
		}
		Collections.sort(pairs, (a, b) -> a.col.compareTo(b.col));
		for (int i = 0; i < pairs.size(); i++) {
			cols[i] = pairs.get(i).col;
			vals[i] = pairs.get(i).val;
		}
	}

	public static boolean checkRecords(String[] record1, String[] record2) {
		for (int i = 0; i < record1.length; i++) {
			if (!record1[i].equals(record2[i])) {
				return false;
			}
		}
		return true;
	}

	public static ArrayList<String[]> getRecords(String bitMap, Table t) {
		ArrayList<String[]> result = new ArrayList<String[]>();
		ArrayList<String[]> records = t.tableRecords();
		for (int i = 0; i < bitMap.length(); i++) {
			if (bitMap.charAt(i) == '1') {
				result.add(records.get(i));
			}
		}
		return result;

	}

	public static String bitWiseAnd(String bitMap1, String bitMap2) {
		String result = "";
		for (int i = 0; i < bitMap1.length(); i++) {
			if (bitMap1.charAt(i) == '1' && bitMap2.charAt(i) == '1') {
				result += '1';
			} else {
				result += '0';
			}
		}
		return result;
	}

	public static ArrayList<String[]> AndingRecords(ArrayList<String[]> records, ArrayList<String[]> r) {
		ArrayList<String[]> result = new ArrayList<String[]>();
		for (int i = 0; i < records.size(); i++) {
			for (int j = 0; j < r.size(); j++) {
				if (checkRecords(records.get(i), r.get(j))) {
					result.add(records.get(i));
					break;
				}
			}
		}
		return result;
	}

	public static void main(String[] args) throws IOException {
		FileManager.reset();
		String[] cols = { "id", "name", "major", "semester", "gpa" };
		createTable("student", cols);
		String[] r1 = { "1", "stud1", "CS", "5", "0.9" };
		insert("student", r1);
		String[] r2 = { "2", "stud2", "BI", "7", "1.2" };
		insert("student", r2);
		String[] r3 = { "3", "stud3", "CS", "2", "2.4" };
		insert("student", r3);
		createBitMapIndex("student", "gpa");
		createBitMapIndex("student", "major");
		System.out.println("Bitmap of the value of CS from the major index: " + getValueBits("student", "major", "CS"));
		System.out.println("Bitmap of the value of 1.2 from the gpa index: " + getValueBits("student", "gpa", "1.2"));
		String[] r4 = { "4", "stud4", "CS", "9", "1.2" };
		insert("student", r4);
		String[] r5 = { "5", "stud5", "BI", "4", "3.5" };
		insert("student", r5);
		System.out.println("After new insertions:");
		System.out.println("Bitmap of the value of CS from the major index: " + getValueBits("student", "major", "CS"));
		System.out.println("Bitmap of the value of 1.2 from the gpa index: " + getValueBits("student", "gpa", "1.2"));
		System.out.println("Output of selection using index when all columns of the select conditions are indexed:");
		ArrayList<String[]> result1 = selectIndex("student", new String[] { "major", "gpa" },
				new String[] { "CS", "1.2" });
		for (String[] array : result1) {
			for (String str : array) {
				System.out.print(str + " ");
			}
			System.out.println();
		}
		System.out.println("Last trace of the table: " + getLastTrace("student"));
		System.out.println("--------------------------------");

		System.out.println(
				"Output of selection using index when only one column of the columns of the select conditions are indexed:");
		ArrayList<String[]> result2 = selectIndex("student", new String[] { "major", "semester" },
				new String[] { "CS", "5" });
		for (String[] array : result2) {
			for (String str : array) {
				System.out.print(str + " ");
			}
			System.out.println();
		}
		System.out.println("Last trace of the table: " + getLastTrace("student"));
		System.out.println("--------------------------------");

		System.out.println(
				"Output of selection using index when some of the columns of the select conditions are indexed:");
		ArrayList<String[]> result3 = selectIndex("student", new String[] { "major", "semester", "gpa" },
				new String[] { "CS", "5", "0.9" });
		for (String[] array : result3) {
			for (String str : array) {
				System.out.print(str + " ");
			}
			System.out.println();
		}
		System.out.println("Last trace of the table: " + getLastTrace("student"));
		System.out.println("--------------------------------");

		System.out.println("Full Trace of the table:");
		System.out.println(getFullTrace("student"));
		System.out.println("--------------------------------");
		System.out.println("The trace of the Tables Folder:");
		System.out.println(FileManager.trace());
	}
}
