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
import java.util.List;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class yearCount {

    public static class DBLPMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final Text year = new Text();
        private static final IntWritable one = new IntWritable(1);
        List<String> authors = new ArrayList<String>();
        int iteration=0;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
        	if(iteration==100000)
		{
			return;
		}
	        if (value.toString().contains("<author>"))
	        {
			int startIndex = value.toString().indexOf("<author>") + "<author>".length();
		        int endIndex = value.toString().indexOf("</author>");
		        String yearValue = value.toString().substring(startIndex, endIndex).trim();
		        authors.add(yearValue);
	        }
	        if(value.toString().contains("</article>"))
	        {
	        	List<String> authorslist = new ArrayList<String>(authors);
			for (int i = 0; i < authorslist.size(); i++) 
			{
				for (int j = i+1; j < authorslist.size(); j++) 
				{
			    		year.set(authorslist.get(i)+" , "+authorslist.get(j));
			    		context.write(year,one);
			    	}
			}
			authors.clear();
		}
		iteration=iteration+1;
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
        job.setInputFormatClass(TextInputFormat.class); //textinputformat reads line by line while xmlinputformat reads tag by tag
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
