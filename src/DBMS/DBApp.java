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
		ArrayList<String[]> res = t.select(); // all records cuurrently in the table
		return t.missingRecords(res);
	}

	public static void recoverRecords(String tableName, ArrayList<String[]> missing) {
		Table t = FileManager.loadTable(tableName);
		t.recoverRecords(missing);
		FileManager.storeTable(tableName, t);
	}

	public static void createBitMapIndex(String tableName, String colName) {
		Table t = FileManager.loadTable(tableName);
		BitmapIndex b = new BitmapIndex(t, colName);
		FileManager.storeTableIndex(tableName, colName, b);
	}

	public static String getValueBits(String tableName, String colName, String value) {
		BitmapIndex b = FileManager.loadTableIndex(tableName, colName);
		String res = b.getValueBits(value);

		return res;
	}

	public static ArrayList<String[]> selectIndex(String tableName, String[] cols, String[] vals) {
		Table t = FileManager.loadTable(tableName);
		ArrayList<String[]> result = new ArrayList<String[]>();
		int IndexSize = 0;
		for (int i = 0; i < cols.length; i++) {
			BitmapIndex b = FileManager.loadTableIndex(tableName, cols[i]);
			if (b != null) {
				IndexSize++;
			}
		}
		if (IndexSize == cols.length) {
			String bits = "";
			for(int i = 0; i < cols.length; i++) {
				BitmapIndex b = FileManager.loadTableIndex(tableName, cols[i]);
				String x = b.getValueBits(vals[i]);
				if(bits.length() == 0)
					bits = x;
				else{
					String s = "";
					for(int j = 0; j < bits.length(); j++){
						if(bits.charAt(j) == '1' && x.charAt(j) == '1')
							s += "1";
						else
							s += "0";
					}
					bits = s;
				}
				
				
			}
			ArrayList<String[]> res = t.select();
			for(int i = 0; i < bits.length(); i++){
				if(bits.charAt(i) == '1'){
					result.add(res.get(i));
				}
			}
			
		}
		else if(IndexSize == 1){

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
		String[] r4 = { "4", "stud4", "DMET", "9", "1.2" };
		insert("student", r4);
		String[] r5 = { "5", "stud5", "BI", "4", "3.5" };
		insert("student", r5);
		System.out.println("Output of selecting the whole table content:");
		ArrayList<String[]> result1 = select("student");
		for (String[] array : result1) {
			for (String str : array) {
				System.out.print(str + " ");
			}
			System.out.println();
		}

		System.out.println("--------------------------------");
		System.out.println("Output of selecting the output by position:");
		ArrayList<String[]> result2 = select("student", 1, 1);
		for (String[] array : result2) {
			for (String str : array) {
				System.out.print(str + " ");
			}
			System.out.println();
		}

		System.out.println("--------------------------------");
		System.out.println("Output of selecting the output by column condition:");
		ArrayList<String[]> result3 = select("student", new String[] { "gpa" }, new String[] { "1.2" });
		for (String[] array : result3) {
			for (String str : array) {
				System.out.print(str + " ");
			}
			System.out.println();
		}
		System.out.println("--------------------------------");
		System.out.println("Full Trace of the table:");
		System.out.println(getFullTrace("student"));
		System.out.println("--------------------------------");
		System.out.println("Last Trace of the table:");
		System.out.println(getLastTrace("student"));
		System.out.println("--------------------------------");
		System.out.println("The trace of the Tables Folder:");
		System.out.println(FileManager.trace());
		FileManager.reset();
		System.out.println("--------------------------------");
		System.out.println("The trace of the Tables Folder after resetting:");
		System.out.println(FileManager.trace());
	}

}
