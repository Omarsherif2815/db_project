package DBMS;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.junit.Test;

@SuppressWarnings("unused")
public class DBApp
{
	static int dataPageSize = 2;
	
	public static void createTable(String tableName, String[] columnsNames)
	{
		Table x = new Table(tableName,columnsNames);
		FileManager.storeTable(tableName, x);
	}
	
	public static void insert(String tableName, String[] record)
	{
		if(!Table.tableNames.contains(tableName)){
			System.out.println("Table not found");
			return;
		}
		Table table = FileManager.loadTable(tableName);
		table.insert(record);
		FileManager.storeTable(tableName, table);
	}
	
	public static ArrayList<String []> select(String tableName)
	{
		if(!Table.tableNames.contains(tableName)){
			System.out.println("Table not found");
			return null;
		}
		Table table = FileManager.loadTable(tableName);
		ArrayList<String[]> result = table.getRecords();
		FileManager.storeTable(tableName, table);
		return result;
		
	}
	
	public static ArrayList<String []> select(String tableName, int pageNumber, int recordNumber)
	{
		if(!Table.tableNames.contains(tableName)){
			System.out.println("Table not found");
			return null;
		}
		Table table = FileManager.loadTable(tableName);
		ArrayList<String[]> result = table.select(pageNumber,recordNumber);
		FileManager.storeTable(tableName, table);
		return result;
	}
	
	public static ArrayList<String []> select(String tableName, String[] cols, String[] vals)
	{
		if(!Table.tableNames.contains(tableName)){
			System.out.println("Table not found");
			return null;
		}
		Table table = FileManager.loadTable(tableName);
		ArrayList<String[]> result = table.select(cols,vals);
		if(result.size() == 0){
			System.out.println("invalid column name");
		}
		FileManager.storeTable(tableName, table);
		return result;
	}
	
	public static String getFullTrace(String tableName)
	{
		if(!Table.tableNames.contains(tableName)){
			System.out.println("Table not found");
			return null;
		}
		Table table = FileManager.loadTable(tableName);
		return table.getFullTrace();
	}
	
	public static String getLastTrace(String tableName)
	{
		if(!Table.tableNames.contains(tableName)){
			System.out.println("Table not found");
			return null;
		}
		Table table = FileManager.loadTable(tableName);
		return table.getLastTrace();
	}
	
	
	public static void main(String []args) throws IOException
	{
	String[] cols = {"id","name","major","semester","gpa"};
	createTable("student", cols);
	String[] r1 = {"1", "stud1", "CS", "5", "0.9"};
	insert("student", r1);
	String[] r2 = {"2", "stud2", "BI", "7", "1.2"};
	insert("student", r2);
	String[] r3 = {"3", "stud3", "CS", "2", "2.4"};
	insert("student", r3);
	String[] r4 = {"4", "stud4", "DMET", "9", "1.2"};
	insert("student", r4);
	String[] r5 = {"5", "stud5", "BI", "4", "3.5"};
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
	ArrayList<String[]> result3 = select("student", new String[]{"gpa"}, new
	String[]{"1.2"});
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
		
		
		

