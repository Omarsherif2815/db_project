package DBMS;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

@SuppressWarnings("unused")
public class Table implements Serializable
{
    static ArrayList<String> tableNames = new ArrayList<String>();
	private String name;
    private String[] columnNames;
    private int currentPageCount = 0;
    ArrayList<String> trace = new ArrayList<String>();
    private int recordCount = 0;
    
    public Table(String name, String[] columnNames)
    {
        this.name = name;
        this.columnNames = columnNames;
        tableNames.add(name);
        trace.add("Table created name:"+name+", columnsNames:"+Arrays.toString(columnNames));
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
        trace.add("Inserted:"+Arrays.toString(record)+", at page number:"+ (currentPageCount-1) +", execution time (mil):"+ (System.currentTimeMillis()-startTime)); 
        
    }
    
    public ArrayList<String[]> getRecords(){
        long startTime = System.currentTimeMillis(); 
        ArrayList<String[]> result = new ArrayList<String[]>();
        
        for(int i = 0 ; i < currentPageCount; i++){
            Page p = FileManager.loadTablePage(name, i);
            if(p!=null)
                result.addAll(p.getRecords());
        }
        trace.add("Select all pages:"+currentPageCount+", records:"+recordCount+", execution time (mil):"+(System.currentTimeMillis()-startTime));
        return result;
    }
    
    public ArrayList<String[]> select(int pageNumber, int recordNumber){
        long startTime = System.currentTimeMillis();
        Page page = FileManager.loadTablePage(name, pageNumber);
        ArrayList<String[]> result = new ArrayList<String[]>();
        if(page!=null){
            result.add(page.getRecordByNumber(recordNumber)); 
        }
        trace.add("Select pointer page:"+pageNumber+", record:"+recordNumber+", total output count:"+result.size()+", execution time (mil):"+(System.currentTimeMillis()-startTime));

        return result;
    }
    
    public ArrayList<String[]> select(String[] cols, String[] vals) {
        long startTime = System.currentTimeMillis();
    
        ArrayList<String[]> result = new ArrayList<>();
        StringBuilder recordsPerPage = new StringBuilder("["); 
        int totalMatch = 0;
        int[] columnindex = new int[cols.length];
        for(int i = 0; i < cols.length; i++) {
            for(int j = 0; j < columnNames.length; j++) {
                if (columnNames[j].equalsIgnoreCase(cols[i])) {
                    columnindex[i] = j;
                    break;
                }
            }
        }
        for (int i = 0; i < currentPageCount; i++) {
            Page page = FileManager.loadTablePage(name, i);
            int matchCount = 0;
    
            if (page != null) {
                ArrayList<String[]> pageRecords = page.getRecords(); 
    
                for (String[] record : pageRecords) {
                    boolean match = true;
                    for (int j = 0; j < cols.length && match; j++) {
                        int index = columnindex[j];
                        if (!record[index].equalsIgnoreCase(vals[j])) {
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
    
                
                if (matchCount > 0) {
                    if (recordsPerPage.length() > 1) {
                        recordsPerPage.append(", ");
                    }
                    recordsPerPage.append("[" + i + ", " + matchCount + "]");
                }
            }
        }
    
        recordsPerPage.append("]"); 
    

        String s = Arrays.toString(cols) + "->" + Arrays.toString(vals);
        trace.add("Select condition:" + s + ", Records per page:" + recordsPerPage.toString() +", records:" + totalMatch + ", execution time (mil):" + (System.currentTimeMillis()-startTime));    
        return result;
    }
    
    
    public String getFullTrace(){
        String s = "";
        for(int i = 0; i < trace.size(); i++){
            s += trace.get(i)+"\n";
        }
        return s + "Pages Count: " + currentPageCount + ", Records Count: " + recordCount ;

    }

    public String getLastTrace(){
        return trace.get(trace.size()-1);
    }
}
