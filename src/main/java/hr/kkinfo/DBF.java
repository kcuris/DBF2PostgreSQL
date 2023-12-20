package hr.kkinfo;

import net.iryndin.jdbf.reader.DbfReader;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class DBF extends DbfReader {

    private String name;

    public DBF(File dbfFile) throws IOException {
        super(dbfFile);
    }

    public DBF(File dbfFile, File memoFile) throws IOException {
        super(dbfFile, memoFile);
    }

    public DBF(InputStream dbfInputStream) throws IOException {
        super(dbfInputStream);
    }

    public DBF(InputStream dbfInputStream, String name) throws IOException {
        super(dbfInputStream);
        this.name = name;
    }

    public DBF(InputStream dbfInputStream, InputStream memoInputStream) throws IOException {
        super(dbfInputStream, memoInputStream);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
