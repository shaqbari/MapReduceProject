package com.sist.mapred2;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordDriver {
	int a; 
	
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
	
	//멤버 클래스:(스레드쓸때 많이 쓰인다.) 원래는 이렇게 만들었는데 따로 빼는것이 관리상 좋다.
	public class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable res=new IntWritable();

		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			int sum=0;
			for (IntWritable i : values) {
				sum+=i.get();//IntWritable을 정수형으로 바꾸는 메소드 IntWritable ==> int
				
			}
			
			res.set(sum); // int => IntWritable
			context.write(key, res);
			
			//같은폴더내에 있으면 모두 분석된다.
		}
		
		
	}
	
	public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		/*                      mapper     셔플(쇼트)                    reduce
		 * 1. read a book  =>   read  1  =>   a[1, 1]         =>   a 2
		 * 							        a       1          book[1, 1]            book2
		 * 							        book 1           read[1]                read1
		 * 2. write a book         write 1           write[1]                write1
		 * 							       a		1
		 *     							  book 1
		 *     
		 *     무조건 공백으로 자르는 것이 아니라 필요에 따라 자르는게 달라진다.
		 * 
		 * 		형용사는 다음과 같이 패턴을 만들어준다.
		 * 		맜있[가-힣]+
		 * 
		 * 		블로그는 거의 좋은거만 올라온다.
		 * 		트위터와 페이스북에는 좀 안좋은의견도 올라온다.
		 * 
		 * 		Robot.capture() 검색해보자 java기본클래스이다.
		 * */
		
		private final IntWritable one=new IntWritable(1); //자를때마다 1씩 증가시킨다.
		private Text result=new Text();
		private String[] feel = {"사랑","로맨스","매력","즐거움","스릴",
				"소름","긴장","공포","유머","웃음","개그",
				"행복","전율","경이","우울","절망","신비",
				"여운","희망","긴박","감동","감성","휴머니즘",
				"자극","재미","액션","반전","비극","미스테리",
				"판타지","꿈","설레임","흥미","풍경","일상",
				"순수","힐링","눈물","그리움","호러","충격","잔혹",
				"드라마", "멜로","애정",
				"로맨스", "스릴러","느와르","다큐멘터리",
				"코미디", "범죄","SF", "애니메이션"	};
		
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
				
				Pattern[] p=new Pattern[feel.length];
				for(int a=0; a<p.length; a++){
					
					p[a]=Pattern.compile(feel[a]);// 정규식 문자열을 패턴으로 만들어준다.
				}
				
				//((aaa)(bbb)(ccc)(ddd))     (211).(238).(142).(98) ip전체가져올때 group()으로 가져오고 일부만가져올때는 group1, 2, 3으로 가져온다.
				// ((d{1-3})\\.(d{1-3})\\.(d{1-3})\\.(d{1-3}))
				Matcher[] m=new Matcher[feel.length];
					for(int a=0; a<m.length; a++){
					
					m[a]=p[a].matcher(value.toString()); //라인(한줄)로 들어온 문자열 값과 비교해서 해당하는게 있다면 매쳐를 만들어준다. 
					/*if (m[a].find()) {
						//result.set(feel[a]);
						result.set(m[a].group());
						context.write(result, one);//누적
					}*/
					while (m[a].find()) {//라인에서 
						//result.set(feel[a]);
						result.set(m[a].group());
						context.write(result, one);//누적
					}
				}
				a=10;
		}
		
		
		
	}
}
