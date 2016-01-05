import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Stuck {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

        private final static Text k = new Text();
        private final static IntWritable v = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            String[] line = value.toString().split(",");
            k.set(line[0] + "," + line[1] + "," + line[2] + "," + line[3] + "," + line[4] + "," + line[5] + "," + line[6]);
            v.set((int) Float.parseFloat(line[7]));
            context.write(k,v);
        }
    }

    public static class MyReducer
            extends Reducer<Text,IntWritable,Text,NullWritable> {
            private Text result = new Text();

            public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                int sum = 0;
                int max = Integer.MIN_VALUE, min = Integer.MAX_VALUE;
                for (IntWritable val : values) {
                    int t = val.get();
                    if(min > t) min = t;
                    if(max < t) max = t;
                    sum ++;
                }

                if(sum >= 300){
                    String[] line = key.toString().split(",");
                    result.set("id: " + line[0] + " count = " + sum + ", max = " + max + ", min = " + min);
                    context.write(result, NullWritable.get());
                }
            }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Discovering stuck vehicles");
        job.setJarByClass(Stuck.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
