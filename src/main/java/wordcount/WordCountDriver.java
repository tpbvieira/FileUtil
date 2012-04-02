package wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountDriver {

	public static void main(String[] args) throws IOException,	InterruptedException, ClassNotFoundException {
		System.out.println("### WordCountDriver ###");
		
		Configuration conf = new Configuration();		
		Job job = new Job(conf, "wordcountdriver");
		job.setJarByClass(WordCountMapper.class);
		
		GenericOptionsParser parser = new GenericOptionsParser(conf, args);
		args = parser.getRemainingArgs();
		
		Path inputPath;
		Path outputPath;
		
		if(args.length > 1){
			inputPath = new Path(args[0]);
			outputPath = new Path(args[1]);	
		}else{
			inputPath = new Path("s3://jxtab1/input/txt/");
			outputPath = new Path("s3://jxtab1/output/");
		}
		
		// Input
		FileInputFormat.setInputPaths(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// MapReduce
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		// Output
		String outDir = "/output."+ System.currentTimeMillis();
		FileOutputFormat.setOutputPath(job, new Path(outputPath + outDir));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		job.waitForCompletion(true);
	}
}