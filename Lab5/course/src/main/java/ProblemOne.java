
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 *
 * @author I3OR2A
 */
public class ProblemOne {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        private Text word = new Text();
        private Text info = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            Map map = XMLParserUtils.transformXmlToMap(value.toString());

            String userId = (String) map.get("UserId");
            String creationDate = (String) map.get("CreationDate");

            if (userId != null && creationDate != null) {
                word.set(userId);
                info.set(creationDate + " " + creationDate + " " + 1);
                context.write(word, info);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean isFirst = true;
            int sum = 0;
            String firstDateString = "";
            String lastDateString = "";
            for (Text val : values) {
                try {
                    String[] dates = val.toString().split(" ");
                    sum += Integer.parseInt(dates[2]);

                    if (isFirst) {
                        firstDateString = dates[0];
                        lastDateString = dates[1];
                        isFirst = false;
                        continue;
                    }

                    String curDateString_first = dates[0];
                    String curDateString_last = dates[1];
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
                    Date curDate_first = sdf.parse(curDateString_first);
                    Date curDate_last = sdf.parse(curDateString_last);

                    Date firstDate = sdf.parse(firstDateString);
                    Date lastDate = sdf.parse(lastDateString);
                    if (curDate_last.after(lastDate)) {
                        lastDateString = curDateString_last;
                    }

                    if (curDate_first.before(firstDate)) {
                        firstDateString = curDateString_first;
                    }
                } catch (ParseException ex) {
                    Logger.getLogger(ProblemOne.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            result.set(firstDateString + " " + lastDateString + " " + sum);
            context.write(key, result);
        }
    }

    public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: ProblemOne <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "problem one");
        job.setJarByClass(ProblemOne.class);
        job.setMapperClass(ProblemOne.TokenizerMapper.class);
        job.setCombinerClass(ProblemOne.IntSumReducer.class);
        job.setReducerClass(ProblemOne.IntSumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
