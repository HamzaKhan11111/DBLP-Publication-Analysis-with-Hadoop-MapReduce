import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class yearCount{

    public static class DBLPMapper extends Mapper<LongWritable, Text, Text, IntWritable>
     {
        private static final Text year = new Text();
        private static final IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
            String line = value.toString().trim();
            if (line.contains("<year>"))
             {
                int startIndex = line.indexOf("<year>") + "<year>".length();
                int endIndex = line.indexOf("</year>");
                String yearValue = line.substring(startIndex, endIndex).trim();
                year.set(yearValue);
                context.write(year, one);
            }
        }
    }

    public static class DBLPReducer extends Reducer<Text, IntWritable, Text, IntWritable>
     {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
        {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "DBLP Year Count");
        job.setJarByClass(yearCount.class);
        job.setMapperClass(DBLPMapper.class);
        job.setReducerClass(DBLPReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
