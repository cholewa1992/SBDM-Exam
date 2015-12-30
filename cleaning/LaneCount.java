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

public class LaneCount{

    public static class TokenizerMapper extends Mapper<Object, Text, CompositeKeyWritable, CompositeValueWritable>{

        private static final CompositeKeyWritable k = new CompositeKeyWritable();
        private static final CompositeValueWritable v = new CompositeValueWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            String[] line = value.toString().split(",");

            k.id = Integer.parseInt(line[0]);
            k.time = (long) Float.parseFloat(line[7]);

            v.x = Float.parseFloat(line[1]);
            v.y = Float.parseFloat(line[2]);
            v.s = Float.parseFloat(line[4]);
            v.value = value.toString();

            context.write(k,v);
        }
    }

    public static class IntSumReducer
            extends Reducer<CompositeKeyWritable,CompositeValueWritable,Text,NullWritable> {
            private Text result = new Text();

            public void reduce(CompositeKeyWritable key, Iterable<CompositeValueWritable> values, Context context) throws IOException, InterruptedException {
                CompositeValueWritable prev = null;
                for (CompositeValueWritable t : values) {
                    if(prev != null && d(t.x,t.y, prev.x, prev.y) > prev.s)
                    {
                        result.set("" + key.id);
                        context.write(result, NullWritable.get());
                        return;
                    }
                    prev = t;
                }
            }

            public double d(float lat1, float lon1, float lat2, float lon2){
                double p = Math.PI / 180;
                double a = 0.5 - Math.cos((lat2 - lat1) * p)/2 + 
                    Math.cos(lat1 * p) * Math.cos(lat2 * p) * 
                    (1 - Math.cos((lon2 - lon1) * p))/2;
                return 12742 * Math.asin(Math.sqrt(a)); // 2 * R; R = 6371 km
            }
    }

    public static final class SortReducerByValuesPartitioner extends Partitioner<CompositeKeyWritable,Text> {
        public int getPartition(CompositeKeyWritable key, Text value, int numPartitions) {
            return key.id.hashCode() % numPartitions;
        }
    }

    public static final class CompositeKeyWritable implements WritableComparable<CompositeKeyWritable> {
        private long time;
        private Integer id;

        @Override
        public void readFields(DataInput in) throws IOException {
            time = Long.parseLong(in.readUTF());
            id = Integer.parseInt(in.readUTF());
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF("" + time);
            out.writeUTF("" + id);
        }

        @Override
        public int compareTo(CompositeKeyWritable o){
            int result = this.id.compareTo(o.id);
            if(result == 0) result = -1 * Long.compare(this.time, o.time);
            return result;
        }

    }

    public static final class CompositeValueWritable implements WritableComparable<CompositeValueWritable> {
        private float x;
        private float y;
        private float s;
        private String value;

        @Override
        public void readFields(DataInput in) throws IOException {
            x = Float.parseFloat(in.readUTF());
            y = Float.parseFloat(in.readUTF());
            s = Float.parseFloat(in.readUTF());
            value = in.readUTF();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF("" + x);
            out.writeUTF("" + y);
            out.writeUTF("" + s);
            out.writeUTF(value);
        }

        @Override
        public int compareTo(CompositeValueWritable o){
            return this.value.compareTo(o.value);
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
            return k1.id.compareTo(k2.id);
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
            int result = k1.id.compareTo(k2.id);
            if(result == 0) result = -1 * Long.compare(k1.time, k2.time);
            return result;

        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Lane count");
        job.setJarByClass(LaneCount.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setSortComparatorClass(CompositeKeyOrderingComparator.class);
        job.setGroupingComparatorClass(CompositeKeyGroupingComparator.class);
        job.setPartitionerClass(SortReducerByValuesPartitioner.class);

        job.setMapOutputKeyClass(CompositeKeyWritable.class);
        job.setMapOutputValueClass(CompositeValueWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
