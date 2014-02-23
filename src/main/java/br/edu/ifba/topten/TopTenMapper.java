package br.edu.ifba.topten;

import br.edu.ifba.util.InformationExtractorUtils;
import com.google.common.collect.TreeMultimap;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.xml.stream.XMLStreamException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: marcoscezar
 * Date: 22/02/14
 * Time: 20:48
 * To change this template use File | Settings | File Templates.
 */
public class TopTenMapper extends Mapper<Object, Text, Text, Text> {


    private TreeMultimap postersToRecordMap = TreeMultimap.create();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("TopTenMapper.map()");
        System.out.println("Chave: " + key);
        System.out.println("Valor: " + value);

        StringTokenizer tokenizerObj = new StringTokenizer(value.toString());

        StringBuffer bufferValue = new StringBuffer("");

        while (tokenizerObj.hasMoreTokens()) {

            final String token = tokenizerObj.nextElement().toString();

            if (!StringUtils.isNumeric(token) && StringUtils.isNotBlank(token)) {

                bufferValue.append(token + " ");

            } else {

                if (StringUtils.isNotBlank(token)) {
                    postersToRecordMap.put(new Text(bufferValue.toString().trim()), Integer.parseInt(token));

                    final SortedMap sortedMap = postersToRecordMap.asMap();

                    if (sortedMap.size() > 10) {
                        sortedMap.remove(sortedMap.firstKey());
                    }

                }
            }

        }


    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        final Set<Map.Entry> set = postersToRecordMap.asMap().entrySet();

        for (Map.Entry entry : set) {
            context.write(new Text(entry.getKey().toString()), new Text(entry.getValue().toString()));
        }

    }
}
