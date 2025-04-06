package DBMS;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;

@SuppressWarnings("unused")
public class Page implements Serializable
{
	private ArrayList<String []> data;
    private int maxNumberOfRecords; 

    public Page()
    {
        data = new ArrayList<String[]>();
        maxNumberOfRecords = DBApp.dataPageSize;
    }

    public boolean isFull(){
        return data.size() == maxNumberOfRecords;
    }

    public void insert(String[] record){
        data.add(record);
    }

    public ArrayList<String[]> getRecords(){
        return data;
    }
    public String[] getRecordByNumber(int recordNumber){
        return data.get(recordNumber);
    }
}
