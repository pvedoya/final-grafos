package ar.edu.itba.grafos.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.graphframes.GraphFrame;

import static org.apache.spark.sql.functions.*;

public class QueryManager {
    public static Dataset<Row> executeQuery1(GraphFrame graphFrame) {
        System.out.println("Executing first Query");

        Dataset<Row> noStop = graphFrame
                .filterVertices("labelV='airport'")
                .filterEdges("labelE='route'")
                .find("(origin)-[]->(destination)")
                .filter("origin.id != destination.id")
                .filter("destination.code='SEA'")
                .filter("origin.lat IS NOT NULL and origin.lat < 0")
                .filter("origin.lon IS NOT NULL and origin.lon < 0");

        Dataset<Row> oneStop = graphFrame
                .filterVertices("labelV='airport'")
                .filterEdges("labelE='route'")
                .find("(origin)-[]->(stop); (stop)-[]->(destination)")
                .filter("origin.id != stop.id")
                .filter("stop.id != destination.id")
                .filter("destination.code='SEA'")
                .filter("origin.lat IS NOT NULL and origin.lat < 0")
                .filter("origin.lon IS NOT NULL and origin.lon < 0");

        return noStop.select(
                        col("origin.code"),
                        col("origin.city"),
                        array("origin.code", "destination.code")
                ).union(oneStop.select(
                        col("origin.code"),
                        col("origin.city"),
                        array("origin.code", "stop.code", "destination.code")
        ));
    }

    public static Dataset<Row> executeQuery2(GraphFrame graphFrame) {
        System.out.println("Executing second Query");

        return graphFrame
                .filterEdges("labelE='contains'")
                .find("(continent)-[]->(airport); (country)-[]->(airport)")
                .filter("airport.labelV='airport'")
                .filter("continent.labelV='continent'")
                .filter("country.labelV='country'")
                .select(
                        col("continent.code").as("continentCode"),
                        col("continent.desc").as("continentName"),
                        col("country.code").as("countryCode"),
                        col("country.desc").as("countryName"),
                        col("airport.elev").as("airportElevation")
                )
                .orderBy("continentCode", "countryCode", "airportElevation")
                .groupBy("continentCode", "continentName", "countryCode", "countryName")
                .agg(collect_list("airportElevation"))
                .orderBy("continentCode", "countryCode");
    }
}
