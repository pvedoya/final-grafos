package ar.edu.itba.grafos.utils;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class GraphSchemas {
    public static StructType LoadSchemaForVertices() {
        List<StructField> vertexAttributes = new ArrayList<>();

        vertexAttributes.add(DataTypes.createStructField("id", DataTypes.LongType, false));
        vertexAttributes.add(DataTypes.createStructField("type", DataTypes.StringType, false));
        vertexAttributes.add(DataTypes.createStructField("code", DataTypes.StringType, true));
        vertexAttributes.add(DataTypes.createStructField("icao", DataTypes.StringType, true));
        vertexAttributes.add(DataTypes.createStructField("desc", DataTypes.StringType, true));
        vertexAttributes.add(DataTypes.createStructField("region", DataTypes.StringType, true));
        vertexAttributes.add(DataTypes.createStructField("runways", DataTypes.IntegerType, true));
        vertexAttributes.add(DataTypes.createStructField("longest", DataTypes.IntegerType, true));
        vertexAttributes.add(DataTypes.createStructField("elev", DataTypes.IntegerType, true));
        vertexAttributes.add(DataTypes.createStructField("country", DataTypes.StringType, true));
        vertexAttributes.add(DataTypes.createStructField("city", DataTypes.StringType, true));
        vertexAttributes.add(DataTypes.createStructField("lat", DataTypes.DoubleType, true));
        vertexAttributes.add(DataTypes.createStructField("lon", DataTypes.DoubleType, true));
        vertexAttributes.add(DataTypes.createStructField("author", DataTypes.StringType, true));
        vertexAttributes.add(DataTypes.createStructField("date", DataTypes.StringType, true));
        vertexAttributes.add(DataTypes.createStructField("labelV", DataTypes.StringType, false));

        return DataTypes.createStructType(vertexAttributes);
    }

    public static StructType LoadSchemaForEdges() {
        List<StructField> edgeAttributes = new ArrayList<>();

        edgeAttributes.add(DataTypes.createStructField("id", DataTypes.LongType, false));
        edgeAttributes.add(DataTypes.createStructField("src", DataTypes.LongType, false));
        edgeAttributes.add(DataTypes.createStructField("dst", DataTypes.LongType, false));
        edgeAttributes.add(DataTypes.createStructField("dist", DataTypes.IntegerType, true));
        edgeAttributes.add(DataTypes.createStructField("labelE", DataTypes.StringType, false));

        return DataTypes.createStructType(edgeAttributes);
    }
}
