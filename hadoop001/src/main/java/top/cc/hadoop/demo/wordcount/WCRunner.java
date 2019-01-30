package top.cc.hadoop.demo.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/***
 * 
* @Description:TODO
		程序入口 
		需要指定本次作业使用哪个map 哪个reduce 进行处理
		同时指定数据的路径 和数据存放的路径
		
* @author:   chen.chao
* @time:2019年1月30日 下午5:46:36
 */
public class WCRunner {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		//集群分发需要指明这些jar 的路径  , 因此需要包含一个定位
		job.setJarByClass(WCRunner.class);
		
		//指明使用业务类
		job.setMapperClass(WCMapper.class);
		job.setReducerClass(WCReducer.class);
		
		//指明reducer 的类型 ,基本上mapper 和reduce 相同 
		//因为 其中mapper 是粒度小的处理 而 reducer 是对处理汇总 ,所以reduce 的上层数据流向是job指明 
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		
		//原始数据存放路径 lib 包下面 的类
		FileInputFormat.setInputPaths(job,new Path("/wc/input/"));
		
		//输出数据结果存放
		FileOutputFormat.setOutputPath(job,new Path("/wc/output/"));
		
		//等待集群是否需要提示
		job.waitForCompletion(true);
		
		
	}
}
