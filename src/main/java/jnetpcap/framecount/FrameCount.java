package jnetpcap.framecount;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FrameCount {

	private static final String inputPcap = "s3://jxtab1/input/traces/jxta-sample/";
	private static final String input = "s3://jxtab1/input/txt/";	
	private static final String inputFile = "fileList.txt";
	
	public static void main(String[] args) throws IOException,	InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "jnetpcap.framecount");		
		FileSystem hdfs = FileSystem.get(conf);
		DistributedCache.createSymlink(conf);
				
		System.out.println("### Home = " + hdfs.getHomeDirectory().toString());
		System.out.println("### Work = " + hdfs.getWorkingDirectory().toString());
		
		// Generate input with file's list
		File inputList = new File(inputPcap);
		String[] fileList = inputList.list();
		
		File test = new File("Test.txt");
		System.out.println("### FileList = " + fileList);		
		System.out.println("### AbsolutPath = " + test.getAbsolutePath());
		System.out.println("### Path = " + test.getPath());
		
		PrintWriter file = new PrintWriter(new BufferedWriter(new FileWriter(input + inputFile)));
		for (int i = 0; i < fileList.length; i++) {
			file.println(fileList[i]);
		}
		file.close();
		
		Path dfsFilesPath = new Path(hdfs.getHomeDirectory().toString());
		Path dsfInputPath = new Path(hdfs.getHomeDirectory() + "/input");		
		hdfs.mkdirs(dsfInputPath);	
		
		for (int i = 0; i < fileList.length; i++) {
			Path localFile = new Path(inputPcap + fileList[i]);		
			hdfs.copyFromLocalFile(localFile, dfsFilesPath);
			System.out.println("### from:" + localFile + " to:" + dfsFilesPath );
		}		
		hdfs.copyFromLocalFile(new Path(input + inputFile), dsfInputPath);

		// Input
		FileInputFormat.setInputPaths(job, dsfInputPath);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		//MapReduce
		job.setMapperClass(FrameCountMapper.class);
		job.setReducerClass(FrameCountReducer.class);
		job.setJarByClass(FrameCount.class);

		// Output
		FileOutputFormat.setOutputPath(job, new Path("out." + System.currentTimeMillis()));
		job.setOutputFormatClass(TextOutputFormat.class);		

		// run...
		job.waitForCompletion(true);
	}
}