
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
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
public class ProblemThree {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        private Text word = new Text();
        private Text info = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                StringTokenizer itr = new StringTokenizer(value.toString());
                Map map = XMLParserUtils.transformXmlToMap(value.toString());

                String curDateString = ((String) map.get("CreationDate"));
                String text = (String) map.get("Text");

                if (curDateString != null && text != null) {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
                    Date curDate = sdf.parse(curDateString);

                    Calendar calendar = Calendar.getInstance();
                    calendar.setTime(curDate);
                    int hours = calendar.get(Calendar.HOUR_OF_DAY);

                    word.set(String.valueOf(hours));
                    info.set(text);

                    context.write(word, info);
                }
            } catch (ParseException ex) {
                Logger.getLogger(ProblemTwo.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            ArrayList<Double> arrayList = new ArrayList<>();

            for (Text value : values) {
                arrayList.add((double) value.toString().length());
            }

            Collections.sort(arrayList);

            int count = arrayList.size();
            double median = count % 2 == 0 ? ((double) arrayList.get(count / 2) + (double) arrayList.get((count / 2) - 1)) / 2.0 : (double) arrayList.get(count / 2);
            double deviation_summation = 0.0;
            double total = 0.0;
            for (double value : arrayList) {
                total = total + value;
            }
            double mean = total / (double) count;
            for (double value : arrayList) {
                deviation_summation = deviation_summation + Math.pow(value - mean, 2);
            }
            double deviation_average = deviation_summation / (double) (count - 1);
            double deviation = Math.sqrt(deviation_average);

            result.set(median + " " + deviation);
            context.write(key, result);
        }
    }

    public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: ProblemThree <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "problem three");
        job.setJarByClass(ProblemThree.class);
        job.setMapperClass(ProblemThree.TokenizerMapper.class);
        job.setReducerClass(ProblemThree.IntSumReducer.class);

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
