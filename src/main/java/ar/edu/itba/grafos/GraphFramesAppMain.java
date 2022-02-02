package ar.edu.itba.grafos;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import ar.edu.itba.grafos.utils.GraphSchemas;
import ar.edu.itba.grafos.utils.QueryManager;
import ar.edu.itba.grafos.utils.XMLReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;
import org.xml.sax.SAXException;
import scala.collection.mutable.WrappedArray;

import javax.xml.parsers.ParserConfigurationException;


public class GraphFramesAppMain {

    public static void main(String[] args) throws ParseException, ParserConfigurationException, IOException, SAXException {

        final String startTimestamp = DateTimeFormatter.ofPattern("dd-MM-yyyy-HHmmss").format(LocalDateTime.now());

        SparkConf spark = new SparkConf().setAppName("TP FINAL");
        JavaSparkContext sparkContext= new JavaSparkContext(spark);
        SparkSession session = SparkSession.builder()
                .sparkContext(sparkContext.sc())
                .getOrCreate();
        Path xmlPath = new Path(args[0]);

        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(session);

        XMLReader xmlReader = new XMLReader();
        xmlReader.readGraph(xmlPath);

        List<Row> vertices = xmlReader.loadVertices();
        Dataset<Row> verticesDF = sqlContext.createDataFrame( vertices, GraphSchemas.LoadSchemaForVertices());

        List<Row> edges = xmlReader.loadEdges();
        Dataset<Row> edgesDF = sqlContext.createDataFrame( edges, GraphSchemas.LoadSchemaForEdges() );

        GraphFrame myGraph = GraphFrame.apply(verticesDF, edgesDF);

        String parentFolder = xmlPath.getParent().getParent().toString();

        writeToFileQuery1(QueryManager.executeQuery1(myGraph).collectAsList(),
                new Path(parentFolder + "/" + startTimestamp + "-b1.txt")
        );

        writeToFileQuery2(QueryManager.executeQuery2(myGraph).collectAsList(),
                new Path(parentFolder + "/" + startTimestamp + "-b2.txt")
        );

//        QueryManager.firstQueryTest(myGraph);
//        QueryManager.secondQueryTest(myGraph);

        sparkContext.close();
    }

    private static void writeToFileQuery1(List<Row> results, Path path) throws IOException {
        final Configuration conf = new Configuration();
        final FileSystem fileSystem = FileSystem.get(conf);

        final FSDataOutputStream os = fileSystem.create(path, true);
        os.writeChars("CODE\t ORIGIN\t ROUTE\n");

        for(Row row : results) {

            os.writeChars(row.getAs("originCode").toString() + "\t " +
                            row.getAs("originCity").toString() + "\t " +
                            buildArrayString(row.getAs("route"))
            );
            os.writeChar('\n');
        }
    }

    private static void writeToFileQuery2(List<Row> results, Path path) throws IOException {
        final Configuration conf = new Configuration();
        final FileSystem fileSystem = FileSystem.get(conf);

        final FSDataOutputStream os = fileSystem.create(path, true);
        os.writeChars("CONTINENT\t COUNTRY\t ELEVATIONS\n");

        for(Row row : results) {
            os.writeChars(row.getAs("continentCode").toString() + " (" +
                    row.getAs("continentName").toString() + ")\t " +
                    row.getAs("countryCode").toString() + " (" +
                    row.getAs("countryCode").toString() + ")\t " +
                    buildArrayString(row.getAs("airportElevations"))
            );
            os.writeChar('\n');
        }
    }

    private static String buildArrayString(Object col) {
        WrappedArray<Object> wrappedArray = (WrappedArray<Object>) col;

        StringBuilder sb = new StringBuilder();
        sb.append("[");

        for(int i = 0; i < wrappedArray.length(); i ++) {
            sb.append(wrappedArray.apply(i)).append(',');
        }
        int i = sb.lastIndexOf(",");
        sb.replace(i, i+1, "]");

        return sb.toString();
    }

}