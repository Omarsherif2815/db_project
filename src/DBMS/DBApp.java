package DBMS;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;

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
		ArrayList<String[]> result = new ArrayList<String[]>();
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
			t.updateTrace("Select index condition:" + Arrays.toString(cols) + "->" + Arrays.toString(vals)
					+ ", Indexed columns: " + t.getIndexIndices().toString() + ", Indexed selection count: "
					+ result.size()
					+ ", Final count: " + result.size() + ", execution time (mil):"
					+ (System.currentTimeMillis() - start));
			FileManager.storeTable(tableName, t);

		} else if (IndexSize == 1) { // only one index
			int index = -1;
			ArrayList<String> columnNames = t.getIndexIndices();
			for (int i = 0; i < cols.length; i++) {
				for (int j = 0; j < columnNames.size(); j++) {
					if (columnNames.get(j).equals(cols[i])) {
						index = j;
						break;
					}
				}
			}
			String column = t.getIndexIndices().get(index);
			BitmapIndex b = FileManager.loadTableIndex(tableName, column);
			int resIndex = -1;
			for (int i = 0; i < cols.length; i++) {
				if (column.equals(cols[i])) {
					resIndex = i;
					break;
				}
			}
			String x = b.getValueBits(vals[resIndex]);
			String[] columns = new String[cols.length - 1];
			String[] values = new String[cols.length - 1];
			int newArrayIndex = 0;
			for (int i = 0; i < cols.length; i++) {
				if (!cols[i].equals(column)) {
					columns[newArrayIndex] = cols[i];
					values[newArrayIndex++] = vals[i];
				}
			}
			ArrayList<String> indexed = new ArrayList<String>();
			indexed.add(column);
			ArrayList<String[]> records = getRecords(x, t);
			ArrayList<String[]> r = t.getRecordsWithCondition(columns, values);
			result = AndingRecords(records, r);
			t.updateTrace("Select index condition:" + Arrays.toString(cols) + "->" + Arrays.toString(vals)
					+ ", Indexed columns: " + indexed.toString() + ", Indexed selection count: "
					+ records.size() + ", Non Indexed: " + Arrays.toString(columns)
					+ ", Final count: " + result.size() + ", execution time (mil):"
					+ (System.currentTimeMillis() - start));
			FileManager.storeTable(tableName, t);

		} else if (IndexSize == 0) { // no index
			result = t.selectIndex(cols, vals);
			t.updateTrace("Select index condition:" + Arrays.toString(cols) + "->" + Arrays.toString(vals)
					+ ", Non Indexed: " + Arrays.toString(cols)
					+ ", Final count: " + result.size() + ", execution time (mil):"
					+ (System.currentTimeMillis() - start));
			FileManager.storeTable(tableName, t);
		} else { // some columns are indexed
			ArrayList<String> Bitmap = t.getIndexIndices();
			String bits = "";
			ArrayList<String> indexed = new ArrayList<String>();
			ArrayList<String> valsColumn = new ArrayList<String>();
			for (int i = 0; i < cols.length; i++) {
				if (Bitmap.contains(cols[i])) {
					indexed.add(cols[i]);
					valsColumn.add(vals[i]);
				}
			}
			for (int i = 0; i < indexed.size(); i++) {
				BitmapIndex b = FileManager.loadTableIndex(tableName, indexed.get(i));
				String x = b.getValueBits(valsColumn.get(i));
				if (i == 0)
					bits = x;
				else
					bits = bitWiseAnd(bits, x);
			}
			String[] columns = new String[cols.length - indexed.size()];
			String[] values = new String[cols.length - indexed.size()];
			int newColumnsIndex = 0;
			for (int i = 0; i < cols.length; i++) {
				if (!indexed.contains(cols[i])) {
					columns[newColumnsIndex] = cols[i];
					values[newColumnsIndex++] = vals[i];
				}
			}
			ArrayList<String[]> records = getRecords(bits, t);
			System.out.println(records.toString());
			ArrayList<String[]> r = t.getRecordsWithCondition(columns, values);
			System.out.println(r.toString());
			result = AndingRecords(records, r);
			t.updateTrace("Select index condition:" + Arrays.toString(cols) + "->" + Arrays.toString(vals)
					+ ", Indexed columns: " + indexed.toString() + ", Indexed selection count: "
					+ records.size() + ", Non Indexed: " + Arrays.toString(columns)
					+ ", Final count: " + result.size() + ", execution time (mil):"
					+ (System.currentTimeMillis() - start));
			FileManager.storeTable(tableName, t);
		}

		return result;
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
		String[] r4 = { "4", "stud4", "CS", "9", "1.2" };
		insert("student", r4);
		String[] r5 = { "5", "stud5", "BI", "4", "3.5" };
		insert("student", r5);
		//////// This is the code used to delete pages from the table
		System.out.println("File Manager trace before deleting pages: " + FileManager.trace());
		String path = FileManager.class.getResource("FileManager.class").toString();
		File directory = new File(path.substring(6, path.length() - 17) +
				File.separator
				+ "Tables//student" + File.separator);
		File[] contents = directory.listFiles();
		int[] pageDel = { 0, 2 };
		for (int i = 0; i < pageDel.length; i++) {
			contents[pageDel[i]].delete();
		}
		//////// End of deleting pages code

		System.out.println("File Manager trace after deleting pages: " + FileManager.trace());
		ArrayList<String[]> tr = validateRecords("student");
		System.out.println("Missing records count: " + tr.size());
		recoverRecords("student", tr);
		System.out.println("--------------------------------");
		System.out.println("Recovering the missing records.");
		tr = validateRecords("student");
		System.out.println("Missing record count: " + tr.size());
		System.out.println("File Manager trace after recovering missing records: " + FileManager.trace());
		System.out.println("--------------------------------");
		System.out.println("Full trace of the table: ");
		System.out.println(getFullTrace("student"));
	}
}
