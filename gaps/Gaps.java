import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Gaps{

    public static class TokenizerMapper extends Mapper<Object, Text, CompositeKeyWritable, IntWritable>{

        private static final CompositeKeyWritable k = new CompositeKeyWritable();
        private static final IntWritable v = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            String[] line = value.toString().split(",");

            k.id = Integer.parseInt(line[0]);
            k.time = (int) Float.parseFloat(line[7]);
            v.set(k.time);

            context.write(k,v);
        }
    }

    public static class MyReducer
            extends Reducer<CompositeKeyWritable,IntWritable,Text,NullWritable> {
            private Text result = new Text();

            public void reduce(CompositeKeyWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                int prev = -1;
                for (IntWritable time : values) {
                    //result.set(key.id + " " + key.time);
                    //context.write(result, NullWritable.get());
                    if(prev != -1 && time.get() - prev != 1){
                        result.set("id: " + key.id + " " + prev + "-" + time.get());
                        context.write(result, NullWritable.get());
                    }
                    prev = time.get();
                }
            }
    }

    public static final class IdPartitioner extends Partitioner<CompositeKeyWritable,Text> {
        public int getPartition(CompositeKeyWritable key, Text value, int numPartitions) {
            return key.id % numPartitions;
        }
    }

    public static final class CompositeKeyWritable implements WritableComparable<CompositeKeyWritable> {
        private int time;
        private int id;

        @Override
        public void readFields(DataInput in) throws IOException {
            time = Integer.parseInt(in.readUTF());
            id = Integer.parseInt(in.readUTF());
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF("" + time);
            out.writeUTF("" + id);
        }

        @Override
        public int compareTo(CompositeKeyWritable o){
            int result = Integer.compare(this.id, o.id);
            if(result == 0) result = Integer.compare(this.time, o.time);
            return result;
        }
    }

    public static final class CompositeKeyGroupingComparator extends WritableComparator {
        protected CompositeKeyGroupingComparator(){
            super(CompositeKeyWritable.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2){
            CompositeKeyWritable k1 = (CompositeKeyWritable) w1;
            CompositeKeyWritable k2 = (CompositeKeyWritable) w2;
            return Integer.compare(k1.id, k2.id);
        }
    }

    public static final class CompositeKeyOrderingComparator extends WritableComparator {
        protected CompositeKeyOrderingComparator(){
            super(CompositeKeyWritable.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2){
            CompositeKeyWritable k1 = (CompositeKeyWritable) w1;
            CompositeKeyWritable k2 = (CompositeKeyWritable) w2;
            int result = Integer.compare(k1.id, k2.id);
            if(result == 0) result = Long.compare(k1.time, k2.time);
            return result;

        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Gaps");
        job.setJarByClass(Gaps.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setSortComparatorClass(CompositeKeyOrderingComparator.class);
        job.setGroupingComparatorClass(CompositeKeyGroupingComparator.class);
        job.setPartitionerClass(IdPartitioner.class);

        job.setMapOutputKeyClass(CompositeKeyWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
