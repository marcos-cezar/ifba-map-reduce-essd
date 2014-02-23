package br.edu.ifba.countSumarization;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created with IntelliJ IDEA.
 * User: marcoscezar
 * Date: 22/02/14
 * Time: 20:20
 * To change this template use File | Settings | File Templates.
 */
public class CountSumarizationDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {

        CountSumarizationDriver countDriver = new CountSumarizationDriver();
        int exitCode = ToolRunner.run(countDriver, args);

        System.exit(exitCode);

    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("O Programa deve ser usado assim: CountSumarizationDriver <in> <out>");
            return 2;
        }

        Job job = new Job(getConf());

        job.setJobName(this.getClass().getName());
        job.setJarByClass(CountSumarizationDriver.class);
        job.setMapperClass(PostsPerUserCounterMapper.class);

        job.setOutputKeyClass(TextInputFormat.class);
        job.setOutputValueClass(IntWritable.class);

        job.setReducerClass(CountSumarizationReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        final boolean result = job.waitForCompletion(true);

        return result ? 0 : 1;
    }

}
