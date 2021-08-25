import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Iterator;

public class Main {
    private static final String URL = "jdbc:postgresql://localhost:5432/krit";
    private static final String USER = "postgres";
    private static final String PASSWORD = "1111";
    private static final String TABLE_NAME = "nalog1nom_nazarov";
    private static final String PATH_TO_EXCEL_FILES = "Nalog/To_load";
    private static final String PATH_TO_MOVE_EXCEL_FILES = "Nalog/Loaded";
    private static final int COUNT_CELLS_IN_TABLE = 28;
    private static final int COUNT_CELLS_WILL_IN_DB = 30; //28+2

    public static void main(String[] args){
        try {
            connectToDB();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void connectToDB() throws SQLException {
        Connection connection;
        try{
            connection = DriverManager.getConnection(URL, USER, PASSWORD);
            System.out.println("Connected");
            Statement statement = connection.createStatement();
            createTableIfNotExists(statement);
            moveDataToDB(connection);
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void moveDataToDB(Connection connection) throws Exception {
        connection.setAutoCommit(false);
        String sql = createSqlQueryInsert();
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        File[] files = new File(PATH_TO_EXCEL_FILES).listFiles();
        if (files != null) {
            for (File file : files){
                try(FileInputStream inputStream = new FileInputStream(file)) {
                    Workbook workbook;
                    try {
                        if (file.getName().endsWith("xlsx")) {
                            workbook = new XSSFWorkbook(inputStream);
                        } else if (file.getName().endsWith("xls")) {
                            workbook = new HSSFWorkbook(inputStream);
                        } else {
                            throw new Exception("File name: " + file.getName() + ". File format must be in xls or xlsx");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        continue;
                    }

                    findNeedTableInSheets(connection, preparedStatement, file, workbook);

                    workbook.close();
                }
                catch (Exception e){
                    e.printStackTrace();
                    continue;
                }
                Files.move(file.toPath(), Paths.get(PATH_TO_MOVE_EXCEL_FILES + "/" + file.getName()));

            }
        } else {
            throw new Exception("Directory is empty");
        }
    }

    private static void findNeedTableInSheets(Connection connection, PreparedStatement preparedStatement, File file, Workbook workbook) throws SQLException {
        int countSheets = workbook.getNumberOfSheets();
        Iterator<Row> rowIterator;
        Row nextRow;

        for (int i = 0; i < countSheets; i++) {
            Sheet sheet = workbook.getSheetAt(i);
            rowIterator = sheet.iterator();
            while (rowIterator.hasNext()) {
                nextRow = rowIterator.next();//skip header row
                if (nextRow.getCell(0) != null) {
                    if (nextRow.getCell(0).getCellType().toString().equals("STRING")) {
                        if (nextRow.getCell(0).getStringCellValue().equals("А")) {
                            while (rowIterator.hasNext()){
                                nextRow = rowIterator.next();
                                if (nextRow.getCell(0) == null){
                                    break;
                                }
                                if (nextRow.getCell(0).getCellType().toString().equals("BLANK")
                                        && nextRow.getCell(2).getCellType().toString().equals("BLANK")){
                                    continue;
                                }

                                copyDataFromTableToDB(connection, preparedStatement, file, nextRow);
                            }
                            break;
                        }
                    }
                }
            }
        }
    }

    private static void copyDataFromTableToDB(Connection connection, PreparedStatement preparedStatement, File file, Row nextRow) throws SQLException {
        Iterator<Cell> cellIterator = nextRow.cellIterator();
        int stmtIterator = 1;
        String value;

        for (int j = 0; j < COUNT_CELLS_IN_TABLE; j++) {
            value = "0";

            Cell nextCell = cellIterator.next();
            String cellType = nextCell.getCellType().toString();

            if (cellType.equals("STRING")) {
                value = nextCell.getStringCellValue();
            } else if (cellType.equals("NUMERIC")) {
                value = String.valueOf(nextCell.getNumericCellValue());
            }

            preparedStatement.setString(stmtIterator++, value);
        }

        String[] stringsFileName = file.getName().split("_");
        preparedStatement.setString(stmtIterator++, stringsFileName[0]);
        preparedStatement.setString(stmtIterator, stringsFileName[1].substring(0, 10));

        preparedStatement.addBatch();
        preparedStatement.executeBatch();
        connection.commit();
    }

    private static String createSqlQueryInsert() {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO " + TABLE_NAME + " VALUES (");
        for (int i = 0; i < COUNT_CELLS_WILL_IN_DB; i++) {
            sql.append("?");
            if (i != COUNT_CELLS_WILL_IN_DB -1){
                sql.append(",");
            } else {
                sql.append(")");
            }
        }
        return sql.toString();
    }

    private static void createTableIfNotExists(Statement statement) throws SQLException {
        statement.executeUpdate("CREATE TABLE IF NOT EXISTS \"public\"." + TABLE_NAME + " (  \n" +
                "fielda varchar NULL,  \n" +
                "fieldb varchar NULL,  \n" +
                "fieldv varchar NULL,  \n" +
                "field1 varchar NULL, \n" +
                "field2 varchar NULL,  \n" +
                "field3 varchar NULL,  \n" +
                "field4 varchar NULL,  \n" +
                "field5 varchar NULL, \n" +
                "field6 varchar NULL,  \n" +
                "field7 varchar NULL,  \n" +
                "field8 varchar NULL,  \n" +
                "field9 varchar NULL,  \n" +
                "field10 varchar NULL,  \n" +
                "field11 varchar NULL,  \n" +
                "field12 varchar NULL,  \n" +
                "field13 varchar NULL,  \n" +
                "field14 varchar NULL,  \n" +
                "field15 varchar NULL,  \n" +
                "field16 varchar NULL,  \n" +
                "field17 varchar NULL,  \n" +
                "field18 varchar NULL,  \n" +
                "field19 varchar NULL,  \n" +
                "field20 varchar NULL,  \n" +
                "field21 varchar NULL,  \n" +
                "field22 varchar NULL,  \n" +
                "field23 varchar NULL,  \n" +
                "field24 varchar NULL,  \n" +
                "field25 varchar NULL,  \n" +
                "ter varchar NULL,  \n" +
                "dat varchar NULL \n" +
                "); \n");
    }
}