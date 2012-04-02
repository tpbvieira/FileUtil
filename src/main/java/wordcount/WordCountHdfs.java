package wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountHdfs {

	public static void main(String[] args) throws IOException,	InterruptedException, ClassNotFoundException {
		System.out.println("### WordCountHdfs");
		
		Configuration conf = new Configuration();		
		Job job = new Job(conf, "wordcountdfs");
		
		GenericOptionsParser parser = new GenericOptionsParser(conf, args);
		args = parser.getRemainingArgs();		
		
		// HDFS home directory. 
		// Input and Output arguments must be located at home directory
		FileSystem hdfs = FileSystem.get(conf);
		Path dsfInputPath = new Path(hdfs.getHomeDirectory() + args[0]);
		System.out.println("### Home = " + hdfs.getHomeDirectory());
		System.out.println("### Work = " + hdfs.getWorkingDirectory());
		
		// Input
		FileInputFormat.setInputPaths(job, dsfInputPath);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// MapReduce
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		// Output
		String outDir = "output."+ System.currentTimeMillis();
		Path dsfOutputPath = new Path(hdfs.getHomeDirectory() + args[1] + outDir);
		FileOutputFormat.setOutputPath(job, dsfOutputPath);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Execution
		job.waitForCompletion(true);
	}
}