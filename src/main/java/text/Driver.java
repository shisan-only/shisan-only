package text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1、获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"Average" );
        //2、设置job包路径
        job.setJarByClass(Driver.class);
        //3、获取mapper和reduce
        job.setMapperClass(MapReduce.class);
        job.setReducerClass(reduce.class);
        //4、设置map的输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5、设置最终的输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //6、设置获取的输出、输入路径
        FileInputFormat.setInputPaths(job, new Path("E:\\Big_date\\school\\Text\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\Big_date\\school\\Text\\output2"));
        //7、提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
