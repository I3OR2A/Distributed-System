
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
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

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author User
 */
public class ProblemTwo {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        private Text word = new Text();
        private Text info = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                StringTokenizer itr = new StringTokenizer(value.toString());
                Map map = XMLParserUtils.transformXmlToMap(value.toString());
                
                String curDateString = ((String) map.get("CreationDate"));
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
                Date curDate = sdf.parse(curDateString);
                
                Calendar calendar = Calendar.getInstance();
                calendar.setTime(curDate);
                int hours = calendar.get(Calendar.HOUR_OF_DAY);
                
                word.set(String.valueOf(hours));
                info.set((String) map.get("Text"));
                
                context.write(word, info);
            } catch (ParseException ex) {
                Logger.getLogger(ProblemTwo.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            int total = 0;
            
            for (Text value : values) {
                count += 1;
                total += Integer.parseInt(value.toString());
            }
            
            result.set(String.valueOf(total / count));
            context.write(key, result);
        }
    }

    public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: ProblemTwo <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "problem two");
        job.setJarByClass(ProblemTwo.class);
        job.setMapperClass(ProblemTwo.TokenizerMapper.class);
        job.setCombinerClass(ProblemTwo.IntSumReducer.class);
        job.setReducerClass(ProblemTwo.IntSumReducer.class);

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
