package br.edu.ifba.countSumarization;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created with IntelliJ IDEA.
 * User: marcoscezar
 * Date: 22/02/14
 * Time: 18:43
 * To change this template use File | Settings | File Templates.
 */
public class PostsPerUserCounterMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final IntWritable one = new IntWritable(1);
    private Text word = new Text("");
    private String lastKeyPost = "";


    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("PostsPerUserCounterMapper.map()");
        System.out.println("Chave: " + key);
        System.out.println("Valor: " + value);

        StringTokenizer tokenizer = new StringTokenizer(value.toString());

        StringBuffer bufferValue = new StringBuffer("");
        while (tokenizer.hasMoreTokens()) {

            final String token = tokenizer.nextElement().toString();

            if (!StringUtils.isNumeric(token)) {

                bufferValue.append(token + " ");

            } else {

                System.out.println("Token atual: " + token);
                System.out.println("Nome a ser atribuido: " + bufferValue.toString().trim());
                word.set(new Text(bufferValue.toString().trim()));
                context.write(word, one);
            }

        }



    }
}
