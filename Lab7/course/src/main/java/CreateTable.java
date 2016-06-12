/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 * @author I3OR2A
 */
public class CreateTable {

    /**
     * @param args the command line arguments
     * @throws java.sql.SQLException
     * @throws java.io.FileNotFoundException
     * @throws java.lang.ClassNotFoundException
     */
    public static void main(String[] args) throws SQLException, FileNotFoundException, IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {

        Connection con = DriverManager.getConnection("jdbc:phoenix:cdc-course-05");
        Statement stmt = con.createStatement();

        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS TableHW (id VARCHAR NOT NULL, num INTEGER NOT NULL, val DOUBLE, CONSTRAINT pk PRIMARY KEY (id, num))");

        double min = 0;
        double max = 100;
        double random;

        for (int i = 0; i < 100; ++i) {
            random = ThreadLocalRandom.current().nextDouble(min, max);
            String sql = "UPSERT INTO TableHW VALUES ('aaaaa'," + i + "," + random + ")";
            stmt.executeUpdate(sql);
        }
        for(int i = 100; i < 200; ++i){
            random = ThreadLocalRandom.current().nextDouble(min, max);
            String sql = "UPSERT INTO TableHW VALUES ('bbbbb'," + i + "," + random + ")";
            stmt.executeUpdate(sql);
        }
        for(int i = 200; i < 300; ++i){
            random = ThreadLocalRandom.current().nextDouble(min, max);
            String sql = "UPSERT INTO TableHW VALUES ('ccccc'," + i + "," + random + ")";
            stmt.executeUpdate(sql);
        }
        for(int i = 300; i < 400; ++i){
            random = ThreadLocalRandom.current().nextDouble(min, max);
            String sql = "UPSERT INTO TableHW VALUES ('ddddd'," + i + "," + random + ")";
            stmt.executeUpdate(sql);
        }
        for(int i = 400; i < 500; ++i){
            random = ThreadLocalRandom.current().nextDouble(min, max);
            String sql = "UPSERT INTO TableHW VALUES ('eeeee'," + i + "," + random + ")";
            stmt.executeUpdate(sql);
        }
        for(int i = 500; i < 600; ++i){
            random = ThreadLocalRandom.current().nextDouble(min, max);
            String sql = "UPSERT INTO TableHW VALUES ('fffff'," + i + "," + random + ")";
            stmt.executeUpdate(sql);
        }
        for(int i = 600; i < 700; ++i){
            random = ThreadLocalRandom.current().nextDouble(min, max);
            String sql = "UPSERT INTO TableHW VALUES ('ggggg'," + i + "," + random + ")";
            stmt.executeUpdate(sql);
        }
        for(int i = 700; i < 800; ++i){
            random = ThreadLocalRandom.current().nextDouble(min, max);
            String sql = "UPSERT INTO TableHW VALUES ('hhhhh'," + i + "," + random + ")";
            stmt.executeUpdate(sql);
        }
        for(int i = 800; i < 900; ++i){
            random = ThreadLocalRandom.current().nextDouble(min, max);
            String sql = "UPSERT INTO TableHW VALUES ('iiiii'," + i + "," + random + ")";
            stmt.executeUpdate(sql);
        }
        for(int i = 900; i < 1000; ++i){
            random = ThreadLocalRandom.current().nextDouble(min, max);
            String sql = "UPSERT INTO TableHW VALUES ('jjjjj'," + i + "," + random + ")";
            stmt.executeUpdate(sql);
        }
        con.commit();

        PreparedStatement statement = con.prepareStatement("select * from TableHW");
        ResultSet rset = statement.executeQuery();
        File file = new File("create.txt");
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        BufferedWriter bufferWriter = new BufferedWriter(new OutputStreamWriter(fileOutputStream));
        while (rset.next()) {
            String string = rset.getString("id") + "," + rset.getString("num") + "," + rset.getString("val");
            bufferWriter.write(string);
            bufferWriter.newLine();
        }

        bufferWriter.close();
        statement.close();
        stmt.close();
        con.close();
    }

}
