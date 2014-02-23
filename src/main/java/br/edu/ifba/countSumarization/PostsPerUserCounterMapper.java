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
    private String bufferValue = "";


    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        StringTokenizer tokenizer = new StringTokenizer(value.toString());

        while (tokenizer.hasMoreTokens()) {
            final String token = tokenizer.nextElement().toString();

            if (StringUtils.isNumeric(token)) {

                if (bufferValue.equalsIgnoreCase(token)) {
                    context.write(word, one);
                }
                bufferValue = token;

            } else {

                if (!word.equals("")) {
                    word.set(word + " " + token);
                } else {
                    word.set(token);
                }

            }

        }

    }
}
