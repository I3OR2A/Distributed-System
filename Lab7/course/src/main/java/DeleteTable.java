
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author I3OR2A
 */
public class DeleteTable {

    public static void main(String args[]) throws SQLException {
        Connection con = DriverManager.getConnection("jdbc:phoenix:cdc-course-05");
        Statement stmt = con.createStatement();

        stmt.executeUpdate("DROP TABLE TableHW");

        con.commit();
        
        stmt.close();
        con.close();
    }
}
