package br.edu.ifba.reduceSideJoin;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created with IntelliJ IDEA.
 * User: marcoscezar
 * Date: 19/02/14
 * Time: 15:28
 * To change this template use File | Settings | File Templates.
 */
public class JoinPostersDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        JoinPostersDriver topTen = new JoinPostersDriver();
        int runResult = ToolRunner.run(topTen, args);

        System.exit(runResult);

    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length < 3) {
            System.out.println("JoinPostersDriver deve ser utilizado assim: JoinPostersDriver <in file Users> <in file Posts> <output folder>");
            System.exit(1);
        }

        Job job = new Job(getConf(), "Reduce Side Join");

        job.setJarByClass(JoinPostersDriver.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, UsersMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PostsJoinMapper.class);

//        job.setNumReduceTasks(2);
        job.setReducerClass(UsersPostReducer.class);

        /*Job jobSumarization = new Job(getConf(), "Job Count Sumarization");

        jobSumarization.setJarByClass(JoinPostersDriver.class);
        jobSumarization.setOutputKeyClass(TextInputFormat.class);
        jobSumarization.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(getConf(), new Path());*/


        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        boolean result = job.waitForCompletion(true);

        return result ? 0 : 1;
    }

}
