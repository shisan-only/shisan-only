package text;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

import java.io.IOException;

public class reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//        定义变量进行操作
        int sum = 0;
        int i = 0;
//       进行一个累加操作
        for (IntWritable value : values) {
            sum += value.get();
            i++;
        }
        IntWritable v = new IntWritable(sum/i);
        context.write(key,v);

    }
}
