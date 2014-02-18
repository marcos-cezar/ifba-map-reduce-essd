package br.edu.ifba;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapOutputCollector;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Hello world!
 *
 */
public class FilterBrazilians {


    public static void main( String[] args ) {
        //TODO implementar as chamadas principais.
    }


    public static class FilterBraziliansMapper extends Mapper<Object, Text, NullWritable, String> {

        private String regexFilter;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //HACK: O ideal é pegar de um parametro de conf.
            regexFilter = "bra[zs]il";
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String linha = value.toString();


            if (value.toString().toLowerCase().matches(regexFilter)) {
                context.write(NullWritable.get(), value.toString());
            }
        }
    }


}
