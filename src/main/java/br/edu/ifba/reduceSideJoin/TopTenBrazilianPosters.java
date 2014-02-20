package br.edu.ifba.reduceSideJoin;

import br.edu.ifba.filtering.FilterBraziliansMapper;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: marcoscezar
 * Date: 19/02/14
 * Time: 15:28
 * To change this template use File | Settings | File Templates.
 */
public class TopTenBrazilianPosters extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {


        Job job = new Job();

//        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, );
//        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, );


        return 0;
    }

    public static class TopTenBrazilianPostersMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {




        }
    }


}
