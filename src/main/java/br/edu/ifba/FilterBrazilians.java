package br.edu.ifba;

import br.edu.ifba.util.TransformationUtils;
import org.apache.commons.configuration.XMLConfiguration;
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
import org.xml.sax.InputSource;

import javax.xml.parsers.SAXParser;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.io.StringReader;
import java.util.Map;


public class FilterBrazilians extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
        //TODO implementar as chamadas principais.
        FilterBrazilians filterBrazilians = new FilterBrazilians();
        int exitCode = ToolRunner.run(filterBrazilians, args);

        System.exit(exitCode);
    }

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


        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);

        return success ? 0 : 1;  //To change body of implemented methods use File | Settings | File Templates.
    }


    public static class FilterBraziliansMapper extends Mapper<Object, Text, NullWritable, String> {

        private String regexFilter;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //HACK: O ideal é pegar de um parametro de conf.
            regexFilter = "bra[sz]il";
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String linha = value.toString();

//            final Map<String, String> propertiesOfXml = TransformationUtils.transformXmlToMap(linha);

            String locationExtracted = retrievingLocation(linha);

            System.out.println("O que tem em linha: " + linha);

            if (locationExtracted.equalsIgnoreCase("") == false) {
                if (locationExtracted.toLowerCase().matches(regexFilter)) {
                    //TODO implementar a saída para mostrar o atributo correto do XML.
                    context.write(NullWritable.get(), value.toString());
                }
            }
        }

        private String retrievingLocation(String linha) {
            XPath xpath = XPathFactory.newInstance().newXPath();
            InputSource input = new InputSource(new StringReader(linha));
            try {
                String locationResult = xpath.evaluate("//*[1]/@Location", input);
                System.out.println("Location result: " + locationResult);
                return locationResult;
            } catch (XPathExpressionException e) {
                System.out.println(e.getMessage());
            }
            return "";
        }
    }


}
