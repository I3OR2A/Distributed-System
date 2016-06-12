
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

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author I3OR2A
 */
public class SearchTable {

    public static String[] array_id = {"aaaaa", "bbbbb", "ccccc", "ddddd", "eeeee", "fffff", "ggggg", "hhhhh", "iiiii", "jjjjj"};

    public static void main(String args[]) throws SQLException, FileNotFoundException, IOException {
        Connection con = DriverManager.getConnection("jdbc:phoenix:cdc-course-05");
        String sql_search = "SELECT * FROM TableHW WHERE val = (SELECT MIN(val) FROM TableHW WHERE id = ?)";

        PreparedStatement statement = con.prepareStatement(sql_search);
        File file = new File("search.txt");
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        BufferedWriter bufferWriter = new BufferedWriter(new OutputStreamWriter(fileOutputStream));
        for (int i = 0; i < array_id.length; ++i) {
            statement.clearParameters();
            statement.setString(1, array_id[i]);
            ResultSet rset = statement.executeQuery();
            int ans_num = -1;
            double ans_val = 0.0;
            String ans_id = "";
            while (rset.next()) {
                ans_id = rset.getString("id");
                ans_val = rset.getDouble("val");
                int cur_num = rset.getInt("num");
                ans_num = ans_num > cur_num ? ans_num : cur_num;
            }
            bufferWriter.write(ans_id + "," + ans_num + "," + ans_val);
            bufferWriter.newLine();
        }
 
        bufferWriter.close();
        statement.close();
        con.close();
    }
}
