package br.edu.ifba.countSumarization;

import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.commons.collections.ComparatorUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

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

    private ListMultimap<String, String> dict = ArrayListMultimap.create();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("PostsPerUserCounterMapper.map()");
        System.out.println("Chave: " + key);
        System.out.println("Valor: " + value);

        StringTokenizer tokenizer = new StringTokenizer(value.toString());

        StringBuffer bufferValue = new StringBuffer("");

        while (tokenizer.hasMoreTokens()) {

            final String token = tokenizer.nextElement().toString();

            if (!StringUtils.isNumeric(token) && StringUtils.isNotBlank(token)) {
                bufferValue.append(token + " ");
            } else {
                System.out.println("Token atual: " + token);
                System.out.println("Nome a ser atribuido: " + bufferValue.toString().trim());


                if (StringUtils.isNotBlank(bufferValue.toString())) {
                    word.set(bufferValue.toString());
                    this.dict.put(word.toString(), token);
                }


            }

        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("PostsPerUserCounterMapper.cleanup()");
        final Map<String, Collection<String>> postsPerUser = this.dict.asMap();

        for (Map.Entry<String, Collection<String>> collectionEntry : postsPerUser.entrySet()) {

            final List<String> mapValues = (List<String>) postsPerUser.get(collectionEntry.getKey());

            HashSet<String> valuesWithNoDuplicates = new HashSet<String>(mapValues);

            System.out.println("Chave detentora dos valores: " + collectionEntry.getKey());
            System.out.println("Tamnho dos valores com duplicatas: " + mapValues.size());
            System.out.println("Tamnho dos valores sem duplicatas: " + valuesWithNoDuplicates.size());

            word.set(collectionEntry.getKey());
            context.write(word, new IntWritable(valuesWithNoDuplicates.size()));


        }

    }
}
