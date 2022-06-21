package text;

import jdk.nashorn.internal.ir.CallNode;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapReduce extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text outK = new Text();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        //切割
        String[] split = line.split(" ");
//        抓取我们需要的数据
        outK.set(split[0]);
        IntWritable outV = new IntWritable(Integer.parseInt(split[1]));
//        //循环写出
//        for (String word : words) {
//            //封装
//            outK.set(word);
//
        //写出
        context.write(outK, outV);
    }
}

