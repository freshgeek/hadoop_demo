package top.cc.hadoop.demo.wordcount;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/***
 * 
* @Description:TODO
	Hadoop 对 海量数据 处理方式是 将 内容切块 放入集群中 分发到 Map 处理 然后归总到Reduce 中处理
	
	这是第一个Mapper 程序
	功能统计海量数据中的 单词数量
	
	写Mapper 需要 继承  org.apache.hadoop.mapreduce.Mapper
	其中四个泛型,前两个是hadoop框架指定输入的数据类型,后两个是输出类型
	因为集群是网络环境,所以其中的类型必须是可序列化的类型
	map 和 reduce 的数据输入输出都是以key-value 对的形式封装
	默认情况下,框架传递给我们的mapper的输入数据中,key是要处理文本中一行的起始偏移量,而内容作为value
	[LongWritable 是Hadoop提供的long取代的方法 有自己的序列化方法 更高效]
	输出类型则取决于你需要的数据 在单词统计中 需要的键是单词 值是统计数
	然后重写map
	
* @author:   chen.chao
* @time:2019年1月30日 下午5:16:56
 */
public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	//每读一行调用一次方法
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] words = StringUtils.split(line, " ");
		for (String word : words) {
			//context 是框架提供类 直接写入后框架即可获得
			context.write(new Text(word),new LongWritable(1));
		}
		
	}

}
