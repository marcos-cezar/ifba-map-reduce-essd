package br.edu.ifba.reduceSideJoin;

import br.edu.ifba.filtering.FilterBraziliansMapper;
import br.edu.ifba.util.InformationExtractorUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;

import javax.xml.stream.XMLStreamException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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


        if (args.length < 3) {
            System.out.println("TopTenBrazilianPosters deve ser utilizado assim: TopTenBrazilianPosters <in file> <out folder>");
            System.exit(1);
        }


        Job job = new Job();

        job.setJobName(getClass().getName());
        job.setJarByClass(TopTenBrazilianPosters.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


//        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, );
//        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, );


        return 0;
    }

}
