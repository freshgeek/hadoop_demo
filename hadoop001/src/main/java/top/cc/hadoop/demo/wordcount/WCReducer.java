package top.cc.hadoop.demo.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

	//框架处理完 后将所有的kv对缓存起来,进行分组,然后传递一个组<key,values{}>,调用一次
	//<hello,{1,1,1,1,1,1}>
	@Override
	protected void reduce(Text arg0, Iterable<LongWritable> arg1,
			 Context arg2)
			throws IOException, InterruptedException {
		//汇总逻辑代码
		
		long count = 0;
		for (LongWritable longWritable : arg1) {
			count+=longWritable.get();
		}
		
		//同样输出交给context
		arg2.write(arg0, new LongWritable(count));
		
	}
	
}
