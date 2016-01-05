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

public class Teleportation{

    public static class TokenizerMapper extends Mapper<Object, Text, CompositeKeyWritable, CompositeValueWritable>{

        private static final CompositeKeyWritable k = new CompositeKeyWritable();
        private static final CompositeValueWritable v = new CompositeValueWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            String[] line = value.toString().split(",");

            k.id = Integer.parseInt(line[0]);
            k.time = (long) Float.parseFloat(line[7]);

            v.y = Double.parseDouble(line[1]);
            v.x = Double.parseDouble(line[2]);
            v.s = Double.parseDouble(line[4]);
            v.value = value.toString();

            context.write(k,v);
        }
    }

    public static class MyReducer
            extends Reducer<CompositeKeyWritable,CompositeValueWritable,Text,NullWritable> {
            private Text result = new Text();

            public void reduce(CompositeKeyWritable key, Iterable<CompositeValueWritable> values, Context context) throws IOException, InterruptedException {
                double xp = 0, yp = 0;
                int sum = 0;
                for (CompositeValueWritable t : values) {
                    double d = distance(t.x,t.y, xp, yp);
                    if(xp != 0 && yp != 0 && Math.abs(d - t.s) > 10)
                    {
                        result.set("" + key.id + " - " + key.time + " d: " + distance(t.x,t.y, xp, yp) + " s: " + t.s);
                        context.write(result, NullWritable.get());
                    }
                    xp = t.x;
                    yp = t.y;
                }
            }


            public static double distance(double lat1, double lon1, double lat2,
                    double lon2 ) {

                final int R = 6371; // Radius of the earth

                double latDistance = Math.toRadians(lat2 - lat1);
                double lonDistance = Math.toRadians(lon2 - lon1);
                double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                    + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                    * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
                double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
                double distance = R * c * 1000; // convert to meters

                return distance; 
            }
    }

    public static final class IdPartitioner extends Partitioner<CompositeKeyWritable,Text> {
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
            if(result == 0) result = Long.compare(this.time, o.time);
            return result;
        }
    }

    public static final class CompositeValueWritable implements WritableComparable<CompositeValueWritable> {
        private double x;
        private double y;
        private double s;
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
            if(result == 0) result = Long.compare(k1.time, k2.time);
            return result;

        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Teleportation");
        job.setJarByClass(Teleportation.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setSortComparatorClass(CompositeKeyOrderingComparator.class);
        job.setGroupingComparatorClass(CompositeKeyGroupingComparator.class);
        job.setPartitionerClass(IdPartitioner.class);

        job.setMapOutputKeyClass(CompositeKeyWritable.class);
        job.setMapOutputValueClass(CompositeValueWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
