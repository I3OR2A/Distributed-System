
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

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author User
 */
public class ProblemThree {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
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
            
            ArrayList arrayList = new ArrayList();
            
            for (Text value : values) {
                arrayList.add(Integer.parseInt((value.toString())));
            }
            
            Collections.sort(arrayList);
            
            int count = arrayList.size();
            int median = count % 2 == 0 ? ((int)arrayList.get(count / 2) + (int)arrayList.get((count / 2) - 1)) / 2 : (int)arrayList.get(count / 2);
            double deviation_summation = 0.0;
            for(Text value : values){
                deviation_summation = deviation_summation + Math.pow(Double.parseDouble(value.toString()), 2);
            }
            double deviation_average = deviation_summation / (double) (count - 1);
            int deviation = (int) Math.sqrt(deviation_average);
            
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
