package ar.edu.itba.grafos.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.graphframes.GraphFrame;
import scala.collection.mutable.WrappedArray;

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
                .filter("origin.lon IS NOT NULL and origin.lon < 0")
                .select(
                        col("origin.code").as("originCode"),
                        col("origin.city").as("originCity"),
                        array("origin.code", "destination.code").as("route")
                );

        System.out.println("Direct routes found: " + noStop.count());

        Dataset<Row> oneStop = graphFrame
                .filterVertices("labelV='airport'")
                .filterEdges("labelE='route'")
                .find("(origin)-[]->(stop); (stop)-[]->(destination)")
                .filter("origin.id != stop.id")
                .filter("stop.id != destination.id")
                .filter("destination.code='SEA'")
                .filter("origin.lat IS NOT NULL and origin.lat < 0")
                .filter("origin.lon IS NOT NULL and origin.lon < 0")
                .select(
                        col("origin.code").as("originCode"),
                        col("origin.city").as("originCity"),
                        array("origin.code", "stop.code", "destination.code").as("route")
                );

        System.out.println("One stop routes found: " + oneStop.count());

        Dataset<Row> total = noStop.union(oneStop);
        System.out.println("Total routes (union): " + total.count());

        return total;
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
                .agg(collect_list("airportElevation").as("airportElevations"))
                .orderBy("continentCode", "countryCode");
    }

    public static void firstQueryTest(GraphFrame graphFrame) {
        System.out.println("Executing first test");

        Dataset<Row> destinationSeattle = graphFrame
                .filterVertices("labelV='airport'")
                .filterEdges("labelE='route'")
                .find("(origin)-[]->(destination)")
                .filter("origin.id != destination.id")
                .filter("destination.code='SEA'")
                .select(
                        col("origin.code").as("originCode"),
                        col("origin.city").as("originCity"),
                        col("origin.lat").as("originLatitude"),
                        col("origin.lon").as("originLongitude"),
                        array("origin.code", "destination.code").as("route")
                );

        destinationSeattle.show();

        Dataset<Row> negativeCoordinates = graphFrame
                .filterVertices("labelV='airport'")
                .filterEdges("labelE='route'")
                .find("(origin)-[]->(destination)")
                .filter("origin.id != destination.id")
                .filter("origin.lat IS NOT NULL and origin.lat < 0")
                .filter("origin.lon IS NOT NULL and origin.lon < 0")
                .select(
                        col("origin.code").as("originCode"),
                        col("origin.city").as("originCity"),
                        col("origin.lat").as("originLatitude"),
                        col("origin.lon").as("originLongitude"),
                        array("origin.code", "destination.code").as("route")
                );

        negativeCoordinates.show();

        Dataset<Row> noStop = destinationSeattle.intersect(negativeCoordinates);
        System.out.println("Direct routes found (by intersection): " + noStop.count());
        noStop.show();

        destinationSeattle = graphFrame
                .filterVertices("labelV='airport'")
                .filterEdges("labelE='route'")
                .find("(origin)-[]->(stop); (stop)-[]->(destination)")
                .filter("origin.id != stop.id")
                .filter("stop.id != destination.id")
                .filter("destination.code='SEA'")
                .select(
                        col("origin.code").as("originCode"),
                        col("origin.city").as("originCity"),
                        col("origin.lat").as("originLatitude"),
                        col("origin.lon").as("originLongitude"),
                        array("origin.code", "stop.code", "destination.code").as("route")
                );

        destinationSeattle.show();

        negativeCoordinates = graphFrame
                .filterVertices("labelV='airport'")
                .filterEdges("labelE='route'")
                .find("(origin)-[]->(stop); (stop)-[]->(destination)")
                .filter("origin.id != stop.id")
                .filter("stop.id != destination.id")
                .filter("origin.lat IS NOT NULL and origin.lat < 0")
                .filter("origin.lon IS NOT NULL and origin.lon < 0")
                .select(
                        col("origin.code").as("originCode"),
                        col("origin.city").as("originCity"),
                        col("origin.lat").as("originLatitude"),
                        col("origin.lon").as("originLongitude"),
                        array("origin.code", "stop.code", "destination.code").as("route")
                );

        negativeCoordinates.show();

        Dataset<Row> oneStop = destinationSeattle.intersect(negativeCoordinates);
        System.out.println("One stop routes found (by intersection): " + oneStop.count());

    }

    public static void secondQueryTest(GraphFrame graphFrame) {
        System.out.println("Executing second test");

        Dataset<Row> originalQuery = graphFrame
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
                .agg(collect_list("airportElevation").as("airportElevations"))
                .orderBy("continentCode", "countryCode");

        originalQuery.show(10000);

        Dataset<Row> singleElevations = graphFrame
                .filterEdges("labelE='contains'")
                .find("(continent)-[]->(airport); (country)-[]->(airport)")
                .filter("airport.labelV='airport'")
                .filter("continent.labelV='continent'")
                .filter("country.labelV='country'")
                .select(
                        col("continent.code").as("continentCode"),
                        col("country.code").as("countryCode"),
                        col("airport.elev").as("airportElevation")
                )
                .orderBy("continentCode", "countryCode", "airportElevation");

        singleElevations.show(10000);

    }

}
