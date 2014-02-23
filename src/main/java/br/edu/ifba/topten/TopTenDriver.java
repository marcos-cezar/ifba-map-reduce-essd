package br.edu.ifba.topten;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created with IntelliJ IDEA.
 * User: marcoscezar
 * Date: 23/02/14
 * Time: 17:11
 * To change this template use File | Settings | File Templates.
 */
public class TopTenDriver extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
        System.out.println("TopTenDriver.main()");
        TopTenDriver topTenDriver = new TopTenDriver();

        final int result = ToolRunner.run(topTenDriver, args);

        System.exit(result);

    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length < 2) {
            System.out.println("TopTenDriver deve ser utilizado assim: TopTenDriver <in file Users> <in file " +
                    "Posts> <output folder>");
            System.exit(1);
        }

        Job job = new Job(getConf(), "Top Ten MR");

        job.setJarByClass(TopTenDriver.class);

        job.setMapperClass(TopTenMapper.class);
        job.setReducerClass(TopTenReducer.class);

        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        final boolean result = job.waitForCompletion(true);

        return result ? 0 : 1;

    }
}
