package br.edu.ifba.topten;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: marcoscezar
 * Date: 22/02/14
 * Time: 20:48
 * To change this template use File | Settings | File Templates.
 */
public class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("TopTenMapper.map()");
        System.out.println("Chave: " + key);
        System.out.println("Valor: " + value);


    }
}
