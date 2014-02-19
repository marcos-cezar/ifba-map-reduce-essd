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
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import javax.xml.transform.Source;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Iterator;
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


    public static class FilterBraziliansMapper extends Mapper<Object, Text, NullWritable, String> {

        public static final String XML_TAG_NAME = "row";
        private String regexFilter;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //HACK: O ideal é pegar de um parametro de conf.
            regexFilter = "bra[sz]il";
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String linha = value.toString();

            Map<String, String> xmlAttributes = new HashMap<String, String>();
            try {
                if (linha.contains("<row")) {
                    xmlAttributes = convertXmlBufferToMap(new ByteArrayInputStream(linha
                            .getBytes()));
                }
            } catch (XMLStreamException e) {
                System.out.println(e.getMessage());
            }


            System.out.println("O que tem em linha: " + linha);

            if (xmlAttributes.size() > 0) {
                if (xmlAttributes.containsKey("Location")) {
                    String locationExtracted = xmlAttributes.get("Location");
                    if (locationExtracted.toLowerCase().matches(regexFilter)) {
                        FilterBrazilians.countUsers++;
                        System.out.println("Usuario " + xmlAttributes.get("DisplayName") + " e brasileiro");
                        context.write(NullWritable.get(), xmlAttributes.get("DisplayName"));
                    }
                    System.out.println("Tem no map? " + locationExtracted);

                }
            }

        }

        private Map<String, String> convertXmlBufferToMap(InputStream input) throws XMLStreamException {

            XMLEventReader eventReader = XMLInputFactory.newFactory().createXMLEventReader(input);
            final Map<String, String> attributesMap = new HashMap<String, String>();

            while(eventReader.hasNext()) {
                XMLEvent event = eventReader.nextEvent();

                if (event.isStartElement()) {
                    final StartElement startElement = event.asStartElement();

                    if (startElement.getName().getLocalPart() == XML_TAG_NAME) {
                        Iterator<Attribute> attributes = startElement.getAttributes();

                        while (attributes.hasNext()) {
                            final Attribute attr = attributes.next();
                            attributesMap.put(attr.getName().toString(), attr.getValue().toString());
                        }
                    }

                }

            }

            return attributesMap;
        }

    }


}
