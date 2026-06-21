package hr.kkinfo;

import net.iryndin.jdbf.core.DbfField;
import net.iryndin.jdbf.core.DbfFieldTypeEnum;
import net.iryndin.jdbf.core.DbfRecord;
import org.postgresql.util.PSQLException;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Main {
    public static final String CHARSET = "Windows-1250";
    public static final int DEFAULT_TAX_GROUP = 9;

    public static  String url ;
    public static  String user;
    public static  String password;

    public static void main(String[] args) throws IOException {

        DatabaseConfig config = new DatabaseConfig();
        url = config.getUrl();
        user = config.getUser();
        password = config.getPassword();

        String directoryPath = (args != null && args.length > 0) ? args[0] : System.getProperty("user.dir");
        File directory = new File(directoryPath);

        if (!directory.exists() || !directory.isDirectory()) {
            // Fallback to current working directory of the application
            directoryPath = System.getProperty("user.dir");
            directory = new File(directoryPath);
        }

        List<DBF> dbfFiles = findDbfFiles(directory);

        generateOutputSql(dbfFiles, "output.sql");

        List<String> ddls = createDDLs(dbfFiles);
        List<String> inserts = createInsert(dbfFiles);

        writeSqlToFile(ddls, inserts, "dbCreate.sql");
        writeSqlToDatabase(ddls, inserts);
        System.out.println("done");

    }

    private static void generateOutputSql(List<DBF> dbfFiles, String fileName) throws IOException {
        DBF skladDbf = null;
        DBF barcodeDbf = null;
        DBF trgovciDbf = null;
        for (DBF dbf : dbfFiles) {
            if (dbf.getName().equalsIgnoreCase("SKLAD.DBF")) {
                skladDbf = dbf;
            } else if (dbf.getName().equalsIgnoreCase("BARCODE.DBF")) {
                barcodeDbf = dbf;
            } else if (dbf.getName().equalsIgnoreCase("TRGOVCI.DBF")) {
                trgovciDbf = dbf;
            }
        }

        if (skladDbf == null) {
            System.out.println("SKLAD.DBF not found, skipping output.sql generation.");
            return;
        }

        Map<String, String> barcodeMap = new HashMap<>();
        if (barcodeDbf != null) {
            File barcodeFile = new File(barcodeDbf.getName());
            try (DBF dbf = new DBF(new FileInputStream(barcodeFile), barcodeFile.getName())) {
                DbfRecord record;
                while ((record = dbf.read()) != null) {
                    String sif = getFieldValue(record, "SIF", dbf);
                    String code = getFieldValue(record, "CODE", dbf);
                    if (!sif.isBlank() && !code.isBlank()) {
                        barcodeMap.put(sif, code);
                    }
                }
            }
        }

        File skladFile = new File(skladDbf.getName());
        try (DBF dbf = new DBF(new FileInputStream(skladFile), skladDbf.getName())) {
            List<String> sqls = new ArrayList<>();
            List<String> itemPriceSqls = new ArrayList<>();
            
            sqls.add("-- Display Group");
            sqls.add("INSERT INTO public.display_group (id, name, code, description, icon_id, color, sort_order) " +
                    "VALUES (1, 'Default', 'DEF', 'Default Group', NULL, NULL, 1) ON CONFLICT (id) DO NOTHING;");
            
            itemPriceSqls.add("\n-- Items and Prices");
            
            DbfRecord record;
            while ((record = dbf.read()) != null) {
                String artikal = getFieldValue(record, "artikal", dbf).replace("'", "''");
                String sifra = getFieldValue(record, "sifra", dbf).replace("'", "''");
                String cijenaStr = getFieldValue(record, "cijena", dbf);
                if (cijenaStr.isBlank()) cijenaStr = "0";

                // Check for barcode override
                String code = barcodeMap.getOrDefault(sifra, sifra);

                // Item insert
                String itemSql = String.format(
                    "INSERT INTO public.item (name, code, description, unit_id, for_sale, deposit_refund, tax_group_id, display_group_id, complex) " +
                    "VALUES ('%s', '%s', '%s', 1, true, 0, %d, 1, false);",
                    artikal, code, artikal, DEFAULT_TAX_GROUP
                );
                itemPriceSqls.add(itemSql);

                // Price insert - using subquery to find item_id by code
                String priceSql = String.format(
                    "INSERT INTO public.price (item_id, valid_from, valid_until, price) " +
                    "VALUES ((SELECT id FROM public.item WHERE code = '%s' LIMIT 1), '2026-01-01', NULL, %s);",
                    code, cijenaStr
                );
                itemPriceSqls.add(priceSql);
            }

            sqls.addAll(itemPriceSqls);

            if (trgovciDbf != null) {
                sqls.add("\n-- Users");
                File trgovciFile = new File(trgovciDbf.getName());
                try (DBF tDbf = new DBF(new FileInputStream(trgovciFile), trgovciFile.getName())) {
                    DbfRecord tRecord;
                    while ((tRecord = tDbf.read()) != null) {
                        String ime = getFieldValue(tRecord, "IME", tDbf).replace("'", "''");
                        String oib = getFieldValue(tRecord, "OIBTR", tDbf);
                        
                        String username = ime.toLowerCase().replace(" ", ".");
                        if (username.isBlank()) continue;

                        String userSql = String.format(
                            "INSERT INTO public.users (username, \"name\", oib, email, \"password\", enabled) " +
                            "VALUES ('%s', '%s', '%s', NULL, NULL, true);",
                            username, ime, oib
                        );
                        sqls.add(userSql);
                    }
                }
            }

            try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(fileName)))) {
                for (String sql : sqls) {
                    writer.println(sql);
                }
            }
            System.out.println("Generated " + fileName);
        }
    }

    private static String getFieldValue(DbfRecord record, String fieldName, DBF dbf) throws IOException {
        DbfField field = dbf.getMetadata().getField(fieldName);
        if (field == null) {
            // Try uppercase
            field = dbf.getMetadata().getField(fieldName.toUpperCase());
        }
        if (field == null) return "";
        return new String(Arrays.copyOfRange(record.getBytes(), field.getOffset(), field.getOffset() + field.getLength()), CHARSET).trim();
    }

    private static List<DBF> findDbfFiles(File directory) {
        List<DBF> dbfFiles = new ArrayList<>();

        for (File file : directory.listFiles()) {
            if (file.isFile() && file.getName().endsWith(".dbf") || file.isFile() && file.getName().endsWith(".DBF")) {
                System.out.println("DBF file: " + file.getName());
                try {
                    DBF dbf = new DBF(new FileInputStream(file), file.getName());
                    dbfFiles.add(dbf);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else if (file.isDirectory()) {
                List<DBF> subFiles = findDbfFiles(file);
                dbfFiles.addAll(subFiles);
            }
        }

        return dbfFiles;
    }

    private static List<String> createDDLs(List<DBF> dbfFiles) {
        List<String> ddls = new ArrayList<>();

        for (DBF dbf : dbfFiles) {
            ddls.add(createDropDDL(dbf));
            String ddlsForFile = createDDL(dbf);
//            System.out.println(ddlsForFile);
            ddls.add(ddlsForFile);
        }

        return ddls;
    }

    private static String createDDL(DBF dbf) {
        String tableName = dbf.getName().replace(".DBF", "").replace(".dbf", "");

        StringBuilder ddlString = new StringBuilder("CREATE TABLE " + tableName + " (\n");

        for (DbfField field : dbf.getMetadata().getFields()) {
            String ddlLine = createDDLLine(field);
            ddlString.append(ddlLine);
        }
        ddlString.delete(ddlString.length()-2, ddlString.length());
        ddlString.append("\n);\n");
        return ddlString.toString();
    }

    private static String createDropDDL(DBF dbf) {
        String tableName = dbf.getName().replace(".DBF", "").replace(".dbf", "");
        return "DROP TABLE IF EXISTS " + tableName + "; \n";
    }

    private static List<String> createInsert(List<DBF> dbfFiles) throws IOException {
        List<String> inserts = new ArrayList<>();

        for (DBF dbf : dbfFiles) {
            List<String> insertsForFile = createInsert(dbf);
            inserts.addAll(insertsForFile);
        }
        return inserts;
    }

    private static List<String> createInsert(DBF dbf) throws IOException {
        String tableName = dbf.getName().replace(".DBF", "").replace(".dbf", "");

        String fieldNames = dbf.getMetadata().getFields().stream()
                .map(dbfField -> dbfField.getName())
                .collect(Collectors.joining(", "));

        String insertBase = new StringBuilder(" INSERT INTO " + tableName  + " (" + fieldNames + ") VALUES (").toString();
//        System.out.println("***************" + insertBase );
        List<String> inserts = new ArrayList<>();
        StringBuilder insert = new StringBuilder(insertBase);
        while(true){
            insert = new StringBuilder(insertBase);
            DbfRecord record = dbf.read();
            if (record == null) break;
            for (DbfField field : record.getFields()){
//                System.out.println(" vrijednost : offset:" + field.getOffset() + "length:" + field.getLength());
                String fieldStringValue = new String(Arrays.copyOfRange(record.getBytes(), field.getOffset() , field.getOffset() + field.getLength()), CHARSET).trim();
                if (field.getType().equals(DbfFieldTypeEnum.Numeric)) {
                    if (fieldStringValue.isBlank()) fieldStringValue = "0";
                    insert.append(fieldStringValue + ", ");
                } else if (field.getType().equals(DbfFieldTypeEnum.Date)){
                    System.out.println("fieldStringValue:   DATE = " + fieldStringValue);
                    insert.append("'" + formatDateString(fieldStringValue)+"', ");
                } else if (field.getType().equals(DbfFieldTypeEnum.Logical)){
                    if (fieldStringValue.equalsIgnoreCase("T")){
                        insert.append("TRUE, ");
                    }else{
                        insert.append("FALSE, ");
                    }
                }else{
                    insert.append("'" + fieldStringValue+"', ");
                }
            }
            insert.delete(insert.length()-2, insert.length());
            insert.append(");");
//            System.out.println(insert.toString() );
            inserts.add(insert.toString());
        }
        dbf.close();
        return inserts;
    }
    private static void writeSqlToFile(List<String> ddls, List<String> inserts, String fileName) throws IOException {
        try (OutputStream outputStream = new FileOutputStream(fileName)) {
            for (String ddl : ddls) {
                outputStream.write(ddl.getBytes());
                outputStream.write('\n');
            }
            for (String insert : inserts) {
                outputStream.write(insert.getBytes());
                outputStream.write('\n');
            }
        }
    }

    private static String createDDLLine (DbfField field) {

        DbfFieldTypeEnum type = field.getType();
        String ddlLine = field.getName();
        switch (type) {
            case Character:
                ddlLine +=  String.format(" VARCHAR(%s),\n", field.getLength());
                break;
            case Numeric:
                ddlLine +=  " NUMERIC,\n";
                break;
            case Date:
                ddlLine +=  " DATE,\n";
                break;
            case Logical:
                ddlLine +=  " BOOLEAN,\n";
                break;
            default:
                throw new IllegalArgumentException("Unsupported DBF field type: " + type);
        }
        field.getLength();
        return ddlLine;

    }

    private static void writeSqlToDatabase(List<String> ddls, List<String> inserts) throws IOException {

        Connection conn = null;
        try {
            // Connect to the database
            conn = DriverManager.getConnection(url, user, password);

            // Read SQL statements from file
//            List<String> allLines = Files.readAllLines(Paths.get("migration.sql"));
//            String sql = String.join(" ", allLines);

            // Execute SQL statements
            for (String ddl : ddls){
//                System.out.println(ddl);
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute(ddl);
                }catch (PSQLException e){
                    System.out.println("DDL error: " + ddl);
                    e.printStackTrace();
                }
            }
            for (String insert : inserts){
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute(insert);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            try{
                conn.close();
            }catch (Exception e){
                e.printStackTrace();
            }

        }
    }


    public static String formatDateString(String date) {
        if (date.length()!= 8) return "2000-01-01";
        String year = date.substring(0, 4);
        String month = date.substring(4, 6);
        String day = date.substring(6, 8);
        return year + "-" + month + "-" + day;
    }
}



