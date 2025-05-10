package DBMS;

import java.util.ArrayList;

public class BitmapIndex {
    Table table;
    String columnName;

    BitmapIndex(Table table, String columnName) {
        this.table = table;
        this.columnName = columnName;
    }

    public String getValueBits(String value) {
        int index = table.getColumnIndex(columnName);
        if (index == -1) {
            return "";
        }
        ArrayList<String[]> records = table.select();
        String result = "";
        for (String[] record : records)
            if (record[index].equals(value))
                result += "1";
            else
                result += "0";

        return result;
    }
}
