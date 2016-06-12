
import java.io.BufferedWriter;
import java.io.File;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author I3OR2A
 */
public class TransferTable {

    public static String[] array_id = {"aaaaa", "bbbbb", "ccccc", "ddddd", "eeeee", "fffff", "ggggg", "hhhhh", "iiiii", "jjjjj"};
    public static ArrayList<Datum> arrayList = new ArrayList();

    public static final TableName TABLENAME = TableName.valueOf("HBase");
    public static final byte[] CF = Bytes.toBytes("c1");
    public static final byte[] QUAL1 = Bytes.toBytes("q1");
    public static final byte[] QUAL2 = Bytes.toBytes("q2");

    public static void createTableIfNotExist(Admin admin) throws IOException {
        if (admin.tableExists(TABLENAME)) {
            System.out.println(TABLENAME.getNameAsString() + " exist.");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(TABLENAME);
            HColumnDescriptor colDesc = new HColumnDescriptor(CF);
            tableDesc.addFamily(colDesc);
            admin.createTable(tableDesc);
        }
    }

    public static void upsertData(Table table) throws IOException {
        for (int i = 0; i < arrayList.size(); ++i) {
            Datum datum = arrayList.get(i);
            Put p = new Put(Bytes.toBytes(datum.getId()));
            p.addColumn(CF, QUAL1, Bytes.toBytes(datum.getNum()));
            p.addColumn(CF, QUAL2, Bytes.toBytes(datum.getVal()));
            table.put(p);
        }
    }

    public static void getAllRows(Table table) throws IOException {
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);

        File file = new File("transfer.txt");
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        BufferedWriter bufferWriter = new BufferedWriter(new OutputStreamWriter(fileOutputStream));

        for (Result result : resultScanner) {
            for (Cell cell : result.rawCells()) {
                bufferWriter.write(CellUtil.toString(cell, true));
                bufferWriter.newLine();
            }
        }

        bufferWriter.close();
        fileOutputStream.close();
    }

    public static void deleteTableIfExists(Admin admin) throws IOException {
        if (admin.tableExists(TABLENAME)) {
            admin.disableTable(TABLENAME);
            admin.deleteTable(TABLENAME);
        }
    }

    public static void main(String args[]) throws SQLException, FileNotFoundException, IOException {
        java.sql.Connection con = DriverManager.getConnection("jdbc:phoenix:cdc-course-05");
        String sql_search = "SELECT * FROM TableHW WHERE val = (SELECT MIN(val) FROM TableHW WHERE id = ?)";

        PreparedStatement statement = con.prepareStatement(sql_search);
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
            Datum datum = new Datum(ans_id, ans_num, ans_val);
            arrayList.add(datum);
        }

        statement.close();
        con.close();

        Configuration conf = HBaseConfiguration.create();
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
            try (Admin admin = conn.getAdmin()) {
                try (Table table = conn.getTable(TABLENAME)) {
                    createTableIfNotExist(admin);
                    upsertData(table);
                    getAllRows(table);
                    deleteTableIfExists(admin);
                }
            }
        }
    }
}
