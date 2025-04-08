package DBMS;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

@SuppressWarnings("unused")
public class Table implements Serializable
{
	private String name;
    private String[] columnNames;
    private int currentPageCount = 0;
    private String lastTrace;
    private String fullTrace;
    private int recordCount = 0;
    
    public Table(String name, String[] columnNames)
    {
        this.name = name;
        this.columnNames = columnNames;
        fullTrace = "Table created name:"+name+", columnsNames:"+Arrays.toString(columnNames)+"]\n";
        lastTrace = fullTrace;
    }
    
    public void insert(String[] record)
    {
        long startTime = System.currentTimeMillis();
        Page page = null;
        if(currentPageCount != 0)
            page = FileManager.loadTablePage(name, currentPageCount-1);
        if(page == null || page.isFull()){
            Page p = new Page();
            FileManager.storeTablePage(name, currentPageCount, p);
            currentPageCount++;
            page = p;
        }
        page.insert(record);
        recordCount++;
        FileManager.storeTablePage(name, currentPageCount-1, page);
        lastTrace = "Inserted:"+Arrays.toString(record)+", at page number:"+ (currentPageCount-1) +", execution time (mil):"+ (System.currentTimeMillis()-startTime);
        fullTrace += lastTrace + "\n"; 
    }
    
    public ArrayList<String[]> getRecords(){
        long startTime = System.currentTimeMillis(); 
        ArrayList<String[]> result = new ArrayList<String[]>();
        
        for(int i = 0 ; i < currentPageCount; i++){
            Page p = FileManager.loadTablePage(name, i);
            if(p!=null)
                result.addAll(p.getRecords());
        }
        lastTrace = "Select all pages:"+currentPageCount+", records:"+recordCount+", execution time (mil):"+(System.currentTimeMillis()-startTime);
        fullTrace += lastTrace+"\n";

        return result;
    }
    
    public ArrayList<String[]> select(int pageNumber, int recordNumber){
        if(pageNumber < 0 || pageNumber >= currentPageCount || recordNumber < 0) 
            return null;
        long startTime = System.currentTimeMillis();
        Page page = FileManager.loadTablePage(name, pageNumber);
        ArrayList<String[]> result = new ArrayList<String[]>();
        if(page!=null){
            String[] record = page.getRecordByNumber(recordNumber);
            if(record == null)
                return null;
            result.add(record);

            lastTrace = "Select pointer page:"+pageNumber+", record:"+recordNumber+", total output count:"+result.size()+", execution time (mil):"+(System.currentTimeMillis()-startTime);
            fullTrace += lastTrace+"\n";
        }

        return result;
    }
    
    public ArrayList<String[]> select(String[] cols, String[] vals) {
        long startTime = System.currentTimeMillis();
    
        ArrayList<String[]> result = new ArrayList<>();
        StringBuilder recordsPerPage = new StringBuilder("["); // String to store records per page info
        int totalMatch = 0;
    
        for (int i = 0; i < currentPageCount; i++) {
            Page page = FileManager.loadTablePage(name, i);
            int matchCount = 0;
    
            if (page != null) {
                ArrayList<String[]> pageRecords = page.getRecords(); // Assuming you have a method to get page records
    
                for (String[] record : pageRecords) {
                    boolean match = true;
                    for (int j = 0; j < cols.length && match; j++) {
                        int columnIndex = -1;
    
                        // Find the corresponding column index
                        for (int k = 0; k < columnNames.length; k++) {
                            if (columnNames[k].equalsIgnoreCase(cols[j])) {
                                columnIndex = k;
                                break;
                            }
                        }
    
                        if (columnIndex == -1 || !record[columnIndex].equalsIgnoreCase(vals[j])) {
                            match = false;
                            break;
                        }
                    }
    
                    if (match) {
                        result.add(record);
                        matchCount++;
                        totalMatch++;
                    }
                }
    
                // If we have matching records for this page, add it to the trace
                if (matchCount > 0) {
                    if (recordsPerPage.length() > 1) {
                        recordsPerPage.append(", ");
                    }
                    recordsPerPage.append("[" + i + ", " + matchCount + "]");
                }
            }
        }
    
        recordsPerPage.append("]"); // Close the string representation
    

        // Format the condition part of the trace
        String s = Arrays.toString(cols) + "->" + Arrays.toString(vals);
        lastTrace = "Select condition: " + s + ", Records per page:" + recordsPerPage.toString() +
                    ", records:" + totalMatch + ", execution time (mil):" + (System.currentTimeMillis()-startTime);
    
        // Append to full trace
        fullTrace += lastTrace + "\n";
    
        return result;
    }
    
    
    public String getFullTrace(){
        return fullTrace+"Pages Count: "+currentPageCount+", Records Count: "+recordCount;
    }

    public String getLastTrace(){
        return lastTrace;
    }
}
