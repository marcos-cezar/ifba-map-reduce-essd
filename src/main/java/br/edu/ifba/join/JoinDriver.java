package br.edu.ifba.join;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JoinDriver extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
    	JoinDriver joinDriver = new JoinDriver();
        int exitCode = ToolRunner.run(joinDriver, args);

        System.exit(exitCode);
    }

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 3) {
            System.err.println("O Programa deve ser usado assim: JoinDriver <in> <in> <out>");
            System.exit(2);
        }

		Job job = new Job(this.getConf());
        job.setJobName(this.getClass().getName());
        job.setJarByClass(JoinDriver.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setReducerClass(JoinReducer.class);
        
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, JoinUserMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, JoinPostMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
        boolean success = job.waitForCompletion(true);

        return success ? 0 : 1;  //To change body of implemented methods use File | Settings | File Templates.
	}
}