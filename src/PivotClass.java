import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class PivotClass {

    private class PivotMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            int column = 0;
            long keyLong = key.get();

            for (String aSplit : split) {
                context.write(new IntWritable(column), new Text(Long.toString(keyLong) + "," + aSplit));
                column++;
            }
        }
    }

    private class PivotReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            TreeMap<Long, String> map = new TreeMap<Long, String>();
            String line = "";

            for (Text text : values) {
                map.put(Long.parseLong(text.toString().split(",")[0]), text.toString().split(",")[1]);
            }

            for (Map.Entry<Long, String> entry : map.entrySet()) {
                line += entry.getValue() + ",";
            }

            line = line.substring(0, line.length() - 1);

            context.write(key, new Text(line));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration, "pivot");
        job.setJarByClass(PivotClass.class);
        job.setMapperClass(PivotMapper.class);
        job.setReducerClass(PivotReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
