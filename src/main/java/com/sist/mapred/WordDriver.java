package com.sist.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordDriver {
	
	public static void main(String[] args) {
		//이부분은 스프링에서 application-hadoop.xml부분에 설정해서 메모리에 올린다.
		try {
			Configuration conf=new Configuration();
			Job job=Job.getInstance();
			job.setMapperClass(WordMapper.class);
			job.setReducerClass(WordReducer.class);
			job.setJarByClass(WordDriver.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			FileInputFormat.addInputPath(job, new Path("./input"));
			FileOutputFormat.setOutputPath(job, new Path("./output"));
			job.waitForCompletion(true);//실행
			//여기서는 생성된 폴더를 안지우기때문에 실행할때마다 지워야 한다.ㄴ
			
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}	
}
