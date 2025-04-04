package DBMS;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

public class Table implements Serializable {
    private String tableName;
    private int dataPageSize;
    private String[] columnsNames;
    private int recordNumber = 0;
    private int pageCount = 0; // Keep track of number of pages
    private String fullTrace;
    private String lastTrace;

    public Table(String tableName, int dataPageSize, String[] columnsNames) {
        this.tableName = tableName;
        this.dataPageSize = dataPageSize;
        this.columnsNames = columnsNames;

        String s = "[ ";
        for (int i = 0; i < columnsNames.length - 1; i++) {
            s += columnsNames[i] + ", ";
        }
        s += columnsNames[columnsNames.length - 1] + "]";

        fullTrace = "Table created name: " + tableName + " columns names: " + s + "\n";
        lastTrace = fullTrace;
    }

    public void insert(String[] record) {
        long startTime = System.currentTimeMillis();

        Page page = (pageCount > 0) ? FileManager.loadTablePage(tableName, pageCount - 1) : null;
        if (page == null || page.isFull()) {
            page = new Page(dataPageSize);
            FileManager.storeTablePage(tableName, pageCount, page);
            pageCount++;
        }
        page.insert(record);
        FileManager.storeTablePage(tableName, pageCount - 1, page);
        
        recordNumber++;

        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;

        lastTrace = "INSERTED: " + Arrays.toString(record) + " at page " + (pageCount - 1) + " Execution Time: " + executionTime + " ms\n";
        fullTrace += lastTrace;
    }

    public ArrayList<String[]> select() {
        long startTime = System.currentTimeMillis();
        ArrayList<String[]> result = new ArrayList<>();

        for (int i = 0; i < pageCount; i++) {
            Page page = FileManager.loadTablePage(tableName, i);
            if (page != null) {
                result.addAll(page.selectPage());
            }
        }

        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;

        lastTrace = "Select all pages:" + pageCount + " records:" + recordNumber + " execution time:" + executionTime + " ms\n";
        fullTrace += lastTrace;
        
        return result;
    }

    public ArrayList<String[]> select(int pageNumber, int recordNumber) {
        long startTime = System.currentTimeMillis();

        ArrayList<String[]> result = new ArrayList<>();
        Page page = FileManager.loadTablePage(tableName, pageNumber);
        if (page != null) {
            result.add(page.select(recordNumber));
        }

        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;

        lastTrace = "Select pointer page:" + pageNumber + " record:" + recordNumber + " total output count: " + result.size() + " execution time: " + executionTime + "\n";
        fullTrace += lastTrace;
        
        return result;
    }

    public ArrayList<String[]> select(String[] cols, String[] vals) {
        long startTime = System.currentTimeMillis();
    
        ArrayList<String[]> result = new ArrayList<>();
        StringBuilder recordsPerPage = new StringBuilder("["); // String to store records per page info
    
        for (int i = 0; i < pageCount; i++) {
            Page page = FileManager.loadTablePage(tableName, i);
            int matchCount = 0;
            
            if (page != null) {
                for (String[] record : page.selectPage()) {
                    boolean match = true;
                    for (int j = 0; j < cols.length; j++) {
                        int columnIndex = -1;
                        for (int k = 0; k < columnsNames.length; k++) {
                            if (columnsNames[k].trim().equalsIgnoreCase(cols[j].trim())) {
                                columnIndex = k;
                                break;
                            }
                        }
                        if (columnIndex == -1 || !record[columnIndex].equals(vals[j])) {
                            match = false;
                            break;
                        }
                    }
                    if (match) {
                        result.add(record);
                        matchCount++;
                    }
                }
            }
            
            if (matchCount > 0) {
                if (recordsPerPage.length() > 1) {
                    recordsPerPage.append(", ");
                }
                recordsPerPage.append("[" + i + ", " + matchCount + "]");
            }
        }
    
        recordsPerPage.append("]"); // Close the string representation
        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;
    
        String s = Arrays.toString(cols) + "->" + Arrays.toString(vals);
        lastTrace = "Select condition: " + s + ", Records per page: " + recordsPerPage.toString() + ", records:" + result.size() + " execution time: " + executionTime + " ms\n";
        fullTrace += lastTrace;
        
        return result;
    }
    

    public String getFullTrace() {
        fullTrace += "Pages Count: " + pageCount + ", Records Count: " + recordNumber + "\n";
        return fullTrace;
    }

    public String getLastTrace() {
        return lastTrace;
    }
}
