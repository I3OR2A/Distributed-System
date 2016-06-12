
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestHdfs {
    public static void main(String[] args) throws IOException {
        String data = "hello world";
        FileSystem fs = FileSystem.get(new Configuration());
        double random = Math.random();
        Path dst = new Path("/tmp", String.valueOf(random));
        writeTo(fs, data, dst);
        String copyData = readFrom(fs, dst);
        delete(fs, dst);
        if (!copyData.equals(data)) {
            throw new RuntimeException("The origin data is \"" + data + "\""
                + ", but the data copied from hdfs is \"" + copyData + "\"");
        } else {
            System.out.println("You're the Best");
        }
    }
    private static void getFileStatus(final FileSystem fs, final Path file) throws IOException {
        showFileStatus(fs, fs.getFileStatus(file));
    }
    private static void listDirectory(final FileSystem fs, final Path dir) throws IOException {
        for (FileStatus status : fs.listStatus(dir)) {
            showFileStatus(fs, status);
        }
    }
    private static void showFileStatus(final FileSystem fs, final FileStatus status) throws IOException {
        System.out.println("size:" + status.getLen());
        System.out.println("access time:" + status.getAccessTime());
        System.out.println("Is directory?:" + status.isDirectory());
        System.out.println("Is file?:" + status.isFile());
    }
    private static void delete(final FileSystem fs, final Path target) throws IOException {
        fs.delete(target, false);
    }
    private static String readFrom(final FileSystem fs, final Path source) throws IOException {
        try (DataInputStream input = new DataInputStream(fs.open(source))) {
            return input.readUTF();
        }
    }
    private static void writeTo(final FileSystem fs, final String data, final Path dst) throws IOException {
        try (DataOutputStream output = new DataOutputStream(fs.create(dst))) {
            output.writeUTF(data);
        }
    }
}
