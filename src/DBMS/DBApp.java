package DBMS;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;

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
		ArrayList<String[]> res = t.tableRecords(); // all records cuurrently in the table
		return t.missingRecords(res);
	}

	public static void recoverRecords(String tableName, ArrayList<String[]> missing) {
		Table t = FileManager.loadTable(tableName);
		t.recoverRecords(missing);
		FileManager.storeTable(tableName, t);
	}

	public static void createBitMapIndex(String tableName, String colName) {
		Table t = FileManager.loadTable(tableName);
		if(t == null) {
			System.out.println("Table not found");
			return;
		}
		BitmapIndex b = new BitmapIndex(t, colName);
		t.updateIndexNumber();
		t.setIndexNumber(colName);
		FileManager.storeTable(tableName, t);
		FileManager.storeTableIndex(tableName, colName, b);
	}

	public static String getValueBits(String tableName, String colName, String value) {
		System.out.println("Getting the bitmap of the value " + value + " from the column " + colName);
		BitmapIndex b = FileManager.loadTableIndex(tableName, colName);
		String res = b.getValueBits(value);

		return res;
	}

	public static ArrayList<String[]> selectIndex(String tableName, String[] cols, String[] vals) {
		Table t = FileManager.loadTable(tableName);
		ArrayList<String[]> result = new ArrayList<String[]>();
		int IndexSize = t.getIndexNumber();
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

		} else if (IndexSize == 1) { // only one index
			String column = t.getIndexIndices().get(0);
			BitmapIndex b = FileManager.loadTableIndex(tableName, column);
			String x = b.getValueBits(vals[0]);
			String[] columns = new String[cols.length - 1];
			String[] values = new String[cols.length - 1];
			for (int i = 0; i < cols.length; i++) {
				if (!cols[i].equals(column)) {
					columns[i] = cols[i];
					values[i] = vals[i];
				}
			}
			ArrayList<String[]> records = getRecords(x, t);
			ArrayList<String[]> r = t.getRecordsWithCondition(columns, vals);
			result = AndingRecords(records, r);

		} else if (IndexSize == 0) { // no index
			result = t.getRecordsWithCondition(cols, vals);
		}

		else { // some columns are indexed
			ArrayList<String> Bitmap = t.getIndexIndices();
			String bits = "";
			for (int i = 0; i < Bitmap.size(); i++) {
				BitmapIndex b = FileManager.loadTableIndex(tableName, Bitmap.get(i));
				String x = b.getValueBits(vals[i]);
				if (i == 0)
					bits = x;
				else
					bits = bitWiseAnd(bits, x);
			}
			String[] columns = new String[cols.length - Bitmap.size()];
			String[] values = new String[cols.length - Bitmap.size()];
			for (int i = 0; i < cols.length; i++) {
				for (int j = 0; j < Bitmap.size(); j++) {
					if (!cols[i].equals(Bitmap.get(j)))
						columns[i] = cols[i];
					values[i] = vals[i];
				}
			}
			ArrayList<String[]> r = t.getRecordsWithCondition(columns, values);
			result = AndingRecords(getRecords(bits, t), r);
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
				result += "1";
			} else {
				result += "0";
			}
		}
		return result;
	}

	public static ArrayList<String[]> AndingRecords(ArrayList<String[]> records, ArrayList<String[]> r) {
		ArrayList<String[]> result = new ArrayList<String[]>();
		for (int i = 0; i < records.size(); i++) {
			for (int j = 0; j < r.size(); j++) {
				boolean flag = checkRecords(records.get(i), r.get(j));
				if (flag)
					result.add(records.get(i));

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
		System.out.println("Bitmap of the value of CS from the major index:") ;
		System.out.println("Bitmap of the value of CS from the major index:" + getValueBits("student", "major", "CS"));
		System.out.println("Bitmap of the value of 1.2 from the gpa index:" + getValueBits("student", "gpa", "1.2"));
		String[] r4 = { "4", "stud4", "CS", "9", "1.2" };
		insert("student", r4);
		String[] r5 = { "5", "stud5", "BI", "4", "3.5" };
		insert("student", r5);
		System.out.println("After new insertions:");
		System.out.println("Bitmap of the value of CS from the major index:" + getValueBits("student", "major", "CS"));
		System.out.println("Bitmap of the value of 1.2 from the gpa index:" + getValueBits("student", "gpa", "1.2"));
		System.out.println("Output of selection using index when all columns ofthe select conditions are indexed:");
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
