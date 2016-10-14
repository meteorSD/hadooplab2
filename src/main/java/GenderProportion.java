import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GenderProportion {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, FloatWritable>{

        // im using FloatWritable to have percentage after

        private final static FloatWritable one = new FloatWritable(1);
        private final static FloatWritable zero = new FloatWritable(0);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            // splitting the line
            ArrayList<String> splittedLine = new ArrayList<String>(Arrays.asList(value.toString().split(";")));

            // to have the number of person, i add ones and zeros

            if (splittedLine.get(1).equals("m")) {
                word.set("male");
                context.write(word, one);
                word.set("female");
                context.write(word, zero);
            }
            else {
                word.set("male");
                context.write(word, zero);
                word.set("female");
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,FloatWritable,Text,FloatWritable> {
        private FloatWritable result = new FloatWritable();

        public void reduce(Text key, Iterable<FloatWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            float sum = 0;
            float count = 0;

            // summing the values in the array and counting the number of values in the array to have the percentage

            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }

            Float percentage = new Float(sum/count) ;
            result.set(percentage);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Gender proportion");
        job.setJarByClass(GenderProportion.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}