package DBMS;

import java.io.Serializable;
import java.util.ArrayList;

public class Page implements Serializable
{
    private ArrayList<String[]> records;
    private int maxRecords;

    public Page(int maxRecords)
    {
        this.maxRecords = maxRecords;
        records = new ArrayList<String[]>();
    }

    public boolean isFull()
    {
        return records.size() == maxRecords;
    }

    public boolean insert(String[] record)
    {
        if(!isFull()){
            records.add(record);
            return true;
        }
        return false;
    }
    public ArrayList<String []> selectPage()
    {
        return records;
    }
    public String[] select(int recordNumber)
    {
        return records.get(recordNumber);
    }
    
}
