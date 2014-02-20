package br.edu.ifba.filtering;

import br.edu.ifba.util.InformationExtractorUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import javax.xml.stream.XMLStreamException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class FilterBrazilians extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
        //TODO implementar as chamadas principais.
        FilterBrazilians filterBrazilians = new FilterBrazilians();
        int exitCode = ToolRunner.run(filterBrazilians, args);

        System.exit(exitCode);
    }

    public static int countUsers = 0;

    @Override
    public int run(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("O Programa deve ser usado assim: FilterBrazilians <in> <out>");
            System.exit(2);
        }

        Job job = new Job(getConf());
        job.setJobName(this.getClass().getName());
        job.setJarByClass(FilterBrazilians.class);
        job.setMapperClass(FilterBraziliansMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);

        return success ? 0 : 1;  //To change body of implemented methods use File | Settings | File Templates.
    }



}
