package zavorontii.dmytro.dbf.merger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

import com.linuxense.javadbf.DBFException;
import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFReader;
import com.linuxense.javadbf.DBFWriter;

public class MergeDbfFiles {
    public static void main(String[] args) {

        String file1Path = "file1.dbf";
        String file2Path = "file2.dbf";
        String mergedFilePath = "merged.dbf";

        merge(file1Path, file2Path, mergedFilePath);
    }

    public static void merge(String file1Path, String file2Path, String mergedFilePath){
        try {

            FileInputStream file1InputStream = new FileInputStream(file1Path);
            FileInputStream file2InputStream = new FileInputStream(file2Path);

            DBFReader reader1 = new DBFReader(file1InputStream);
            DBFReader reader2 = new DBFReader(file2InputStream);
            File f = new File(mergedFilePath);
            if (f.getTotalSpace() > 1) f.delete();
            DBFWriter writer = new DBFWriter(f);

            DBFField[] fields = new DBFField[reader1.getFieldCount()];
            for (int i = 0; i < fields.length; i++){
                fields[i] = reader1.getField(i);
            }
            writer.setFields(fields);

            Object[] record;
            while ((record = reader1.nextRecord()) != null) {
                writer.addRecord(record);
            }

            while ((record = reader2.nextRecord()) != null) {
                writer.addRecord(record);
            }

            file1InputStream.close();
            file2InputStream.close();

            System.out.println("DBF merged in " + mergedFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}