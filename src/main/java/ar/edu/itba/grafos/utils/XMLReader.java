package ar.edu.itba.grafos.utils;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.w3c.dom.*;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class XMLReader {
    private NodeList nodeList;
    private NodeList edgeList;

    public XMLReader() {
        nodeList = null;
        edgeList = null;
    }

    public void readGraph(Path xmlPath) throws ParserConfigurationException, IOException, SAXException {
        final Configuration conf = new Configuration();
        final FileSystem fileSystem = FileSystem.get(conf);

        final FSDataInputStream is = fileSystem.open(xmlPath);

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.parse(is);

        doc.getDocumentElement().normalize();
        System.out.println("Root element: " + doc.getDocumentElement().getNodeName());

        nodeList = doc.getElementsByTagName("node");
        edgeList = doc.getElementsByTagName("edge");

        System.out.println("Successfully extracted nodes and edges from xml");
        System.out.println("Node count: " + nodeList.getLength());
        System.out.println("Edge count: " + edgeList.getLength());
        System.out.println();
    }

    public List<Row> loadVertices() {
        ArrayList<Row> vertices = new ArrayList<>();

        for (int i = 0; i < nodeList.getLength(); i++) {
            long id;
            String type = null;
            String code = null;
            String icao = null;
            String desc = null;
            String region = null;
            Integer runways = null;
            Integer longest = null;
            Integer elev = null;
            String country = null;
            String city = null;
            Double lat = null;
            Double lon = null;
            String author = null;
            String date = null;
            String labelV = null;

            Element element = (Element) nodeList.item(i);
            id = Long.parseLong(element.getAttribute("id"));

            NodeList children = element.getElementsByTagName("data");

            for(int j = 0; j < children.getLength(); j++) {
                Element child = (Element) children.item(j);

                switch (child.getAttribute("key")) {
                    case "type":
                        type = child.getTextContent();
                        break;
                    case "code":
                        code = child.getTextContent();
                        break;
                    case "icao":
                        icao = child.getTextContent();
                        break;
                    case "desc":
                        desc = child.getTextContent();
                        break;
                    case "region":
                        region = child.getTextContent();
                        break;
                    case "runways":
                        runways = Integer.valueOf(child.getTextContent());
                        break;
                    case "longest":
                        longest = Integer.valueOf(child.getTextContent());
                        break;
                    case "elev":
                        elev = Integer.valueOf(child.getTextContent());
                        break;
                    case "country":
                        country = child.getTextContent();
                        break;
                    case "city":
                        city = child.getTextContent();
                        break;
                    case "lat":
                        lat = Double.valueOf(child.getTextContent());
                        break;
                    case "lon":
                        lon = Double.valueOf(child.getTextContent());
                        break;
                    case "author":
                        author = child.getTextContent();
                        break;
                    case "date":
                        date = child.getTextContent();
                        break;
                    case "labelV":
                        labelV = child.getTextContent();
                        break;
                }
            }

            if(labelV == null || type == null) {
                throw new IllegalStateException("Id, labelV and type should not be null");
            }

            vertices.add(RowFactory.create(
                    id, type, code, icao, desc, region, runways, longest,
                    elev, country, city, lat, lon, author, date, labelV
            ));
        }

        return vertices;
    }

    public List<Row> loadEdges() {
        ArrayList<Row> edges = new ArrayList<>();

        for (int i = 0; i < edgeList.getLength(); i++) {
            long id;
            long src;
            long dst;
            Integer dist = null;
            String labelE = null;

            Element element = (Element) edgeList.item(i);
            id = Long.parseLong(element.getAttribute("id"));
            src = Long.parseLong(element.getAttribute("source"));
            dst = Long.parseLong(element.getAttribute("target"));

            NodeList children = element.getElementsByTagName("data");

            for (int j = 0; j < children.getLength(); j++) {
                Element child = (Element) children.item(j);

                switch (child.getAttribute("key")) {
                    case "dist":
                        dist = Integer.valueOf(child.getTextContent());
                        break;
                    case "labelE":
                        labelE = child.getTextContent();
                        break;
                }
            }

            if(labelE == null) {
                throw new IllegalStateException("labelE should not be null");
            }

            edges.add(RowFactory.create(id, src, dst, dist, labelE));
        }

        return edges;
    }
}
