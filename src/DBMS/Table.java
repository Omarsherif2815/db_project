package DBMS;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

public class Table implements Serializable
{
	private ArrayList<Page> pages;
    private String tableName;
    private int dataPageSize;
    String[] columnsNames;
    int recordNumber = 0;
    String fullTrace;
    String lastTrace;

    public Table(String tableName, int dataPageSize, String[] columnsNames)
    {
        this.tableName = tableName;
        this.dataPageSize = dataPageSize;
        this.columnsNames = columnsNames;
        pages = new ArrayList<Page>();
        String s = "[ ";
        for(int i = 0; i < columnsNames.length-1; i++)
        {
            s += columnsNames[i].toLowerCase()+ ", ";
        }
        s += columnsNames[columnsNames.length-1].toLowerCase()+"]";
        fullTrace  = "Table created name: "+tableName+" columns names: "+s+"\n";
        lastTrace = "";
    }

    public void insert(String[] record)
    {
        long startTime = System.currentTimeMillis(); 

        if(pages.isEmpty() || pages.get(pages.size()-1).isFull())
        {
            
            Page newPage = new Page(dataPageSize);
            newPage.insert(record);
            pages.add(newPage);
        }
        else
        {
            pages.get(pages.size()-1).insert(record);
        }


        recordNumber++;
            
        long endTime = System.currentTimeMillis();  // End measuring time
        long executionTime = endTime - startTime;   // Calculate execution time
        String s = "[ ";
        for(int i = 0;i<record.length-1;i++)
        {
            s+=record[i]+", ";
        }
        s+=record[record.length-1]+" ]";
        lastTrace = "Inserted: " +s +" at page number: "+ (pages.size()-1)	+"  execution time (mil): "+executionTime+" ms\n";
        fullTrace += lastTrace;
    }


    public ArrayList<String []> select()
{
    long startTime = System.currentTimeMillis();
    ArrayList<String []> result = new ArrayList<>();

    for(Page page : pages)
    {
        result.addAll(page.select());
    }

    long endTime = System.currentTimeMillis();
    long executionTime = endTime - startTime;

    lastTrace = "Select all pages: " + pages.size() + " records: " + recordNumber + " execution time (mil): " + executionTime + "\n";
    
    // Ensure `fullTrace` keeps a history of selects
    fullTrace += lastTrace;  

    return result;
}



    public ArrayList<String []> select(int pageNumber, int recordNumber)
    {
        long startTime = System.currentTimeMillis();

        ArrayList<String []> result = new ArrayList<>();
        result.add(pages.get(pageNumber).select(recordNumber    ));

        long endTime = System.currentTimeMillis();  // End measuring time
        long executionTime = endTime - startTime;   // Calculate execution time

        lastTrace = "Select pointer page: "+pageNumber+" record: "+ result.size()+" total output count"+result.size()+" execution time (mil): "+executionTime+"\n";
        fullTrace += lastTrace;

        return result;
    }


    public ArrayList<String []> select(String[] cols, String[] vals)
    {
        long startTime = System.currentTimeMillis();

        ArrayList<String []> result = new ArrayList<String []>();

        for(Page page : pages)
        {
            for(String[] record : page.select())
            {
                boolean match = true;
                for(int i = 0; i < cols.length; i++)
                {
                    if(!record[Arrays.asList(columnsNames).indexOf(cols[i])].equals(vals[i]))
                    {
                        match = false;
                        break;
                    }
                }
                if(match)
                {
                    result.add(record);
                }
            }
        }

        long endTime = System.currentTimeMillis();  // End measuring time
        long executionTime = endTime - startTime;   // Calculate execution time

        String s = "";
        for(int i = 0;i<cols.length;i++)
        {
            s+=cols[i]+" ->"+vals[i]+" ";
        }

        lastTrace=  "Select condition: "+s+" records: "+result.size()+" execution time (mil): "+executionTime+"\n";
        fullTrace += lastTrace;

        return result;
    }

    public String getFullTrace()
{
    fullTrace += lastTrace + "Pages Count: " + pages.size() + ", Records Count: " + recordNumber + "\n";
    return fullTrace;
}


    public String getLastTrace()
    {
        return lastTrace;
    }
}
