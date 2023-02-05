package ar.edu.itba.graph;

import ar.edu.itba.graph.model.EdgeProp;
import ar.edu.itba.graph.model.VertexProp;
import ar.edu.itba.graph.utils.FileReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
import scala.Tuple2;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class MainApp {


    public static void main(String[] args) throws FileNotFoundException {
        SparkConf spark = new SparkConf().setAppName("TP Final");
        JavaSparkContext sparkContext = new JavaSparkContext(spark);

        SparkSession session = SparkSession.builder()
                .sparkContext(sparkContext.sc())
                .getOrCreate();

        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(session);

        // Read file
        JavaRDD<String> lines = sparkContext.textFile(args[0]);
        List<String> file = lines.collect();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        file.forEach(line -> {
            try {
                baos.write(line.getBytes());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        InputStream in = new ByteArrayInputStream(baos.toByteArray());

        // Load graph from .graphml file
        FileReader fileReader = new FileReader();
        fileReader.read(in);
        List<Tuple2<Object, VertexProp>> verticesTuple = fileReader.getVertices();
        List<Edge<EdgeProp>> edgesTriple = fileReader.getEdges();

        List<Row> verticesGF = LoadVerticesFromTuple(verticesTuple);
        List<Row> edgesGF = LoadEdgesFromTriple(edgesTriple);

        // Load vertices and edges to GraphFrames
        Dataset<Row> verticesDF = sqlContext.createDataFrame(verticesGF, LoadSchemaVertices());
        Dataset<Row> edgesDF = sqlContext.createDataFrame(edgesGF, LoadSchemaEdges());

        GraphFrame airRoutesGraph = GraphFrame.apply(verticesDF, edgesDF);

        // Create graph views
        airRoutesGraph.vertices().createOrReplaceTempView("vertices");
        airRoutesGraph.edges().createOrReplaceTempView("edges");

        // Query b1
        Dataset<Row> directFlightsSEA = airRoutesGraph
                .filterEdges("label = 'route'")
                .filterVertices("type = 'airport'")
                .find("(a)-[e]->(b)")
                .filter("b.code = 'SEA'")
                .filter("a.lat < 0")
                .filter("a.lon < 0")
                .select(col("a.code").as("Airport"),
                        concat(col("a.code"), lit("-"), col("b.code")).as("Route"));

        Dataset<Row> oneStopFlightSEA = airRoutesGraph
                .filterEdges("label = 'route'")
                .filterVertices("type = 'airport'")
                .find("(a)-[e]->(b); (b)-[e2]->(c)")
                .filter("c.code = 'SEA'")
                .filter("a.lat < 0")
                .filter("a.lon < 0")
                .select(col("a.code").as("Airport"),
                        concat(col("a.code"), lit("-"),
                                col("b.code"), lit("-"),
                                col("c.code")).as("Route"));

        Dataset<Row> resultsB1 = directFlightsSEA.union(oneStopFlightSEA);

        // Query b2
        Dataset<Row> resultsB2 = airRoutesGraph
                .find("(a)-[e]->(b); (c)-[e2]->(b)")
                .filter("a.type = 'continent'")
                .filter("c.type = 'country'")
                .filter("b.type = 'airport'")
                .select(col("a.desc").as("Continent"),
                        concat(col("c.code"), lit("("),
                                col("c.desc"), lit(")")).as("Country"),
                        col("b.elev").cast("String").as("elev"))
                .groupBy("Continent", "Country")
                .agg(sort_array(collect_list(col("elev"))).alias("Airport elevations"))
                .orderBy("Continent", "Country");

        //Create file base name for results
        final String baseName = "hdfs:///user/xzuberbuhler/final/results/" +
                DateTimeFormatter.ofPattern("yyyy-MM-dd-HH:mm:ss").format(LocalDateTime.now());

        // Show and save results
        resultsB1.show();
        resultsB2.show();
        resultsB1.rdd().saveAsTextFile(baseName + "-b1");
        resultsB2.rdd().saveAsTextFile(baseName + "-b2");

    }

    public static StructType LoadSchemaVertices()
    {
        List<StructField> vertFields = new ArrayList<StructField>();
        vertFields.add(DataTypes.createStructField("id", DataTypes.LongType, true));
        vertFields.add(DataTypes.createStructField("type", DataTypes.StringType, true));
        vertFields.add(DataTypes.createStructField("code", DataTypes.StringType, true));
        vertFields.add(DataTypes.createStructField("icao", DataTypes.StringType, true));
        vertFields.add(DataTypes.createStructField("desc", DataTypes.StringType, true));
        vertFields.add(DataTypes.createStructField("region", DataTypes.StringType, true));
        vertFields.add(DataTypes.createStructField("runways", DataTypes.IntegerType, true));
        vertFields.add(DataTypes.createStructField("longest", DataTypes.IntegerType, true));
        vertFields.add(DataTypes.createStructField("elev", DataTypes.IntegerType, true));
        vertFields.add(DataTypes.createStructField("country", DataTypes.StringType, true));
        vertFields.add(DataTypes.createStructField("city", DataTypes.StringType, true));
        vertFields.add(DataTypes.createStructField("lat", DataTypes.DoubleType, true));
        vertFields.add(DataTypes.createStructField("lon", DataTypes.DoubleType, true));
        vertFields.add(DataTypes.createStructField("author", DataTypes.StringType, true));
        vertFields.add(DataTypes.createStructField("date", DataTypes.StringType, true));

        return DataTypes.createStructType(vertFields);
    }

    public static StructType LoadSchemaEdges() {
        List<StructField> edgeFields = new ArrayList<>();
        edgeFields.add(DataTypes.createStructField("src",DataTypes.LongType, false));
        edgeFields.add(DataTypes.createStructField("dst",DataTypes.LongType, false));
        edgeFields.add(DataTypes.createStructField("label",DataTypes.StringType, false));
        edgeFields.add(DataTypes.createStructField("dist",DataTypes.IntegerType, true));

        return DataTypes.createStructType(edgeFields);
    }

    public static List<Row> LoadVerticesFromTuple(List<Tuple2<Object, VertexProp>> verticesTuple) {
        return verticesTuple.stream().map(tuple -> {
            VertexProp vertex = tuple._2;
            return RowFactory.create(tuple._1, vertex.getType(), vertex.getCode(), vertex.getIcao(), vertex.getDesc(),
                    vertex.getRegion(), vertex.getRunways(), vertex.getLongest(), vertex.getElev(), vertex.getCountry(),
                    vertex.getCity(), vertex.getLat(), vertex.getLon(), vertex.getAuthor(), vertex.getDate());
        }).collect(Collectors.toList());
    }

    public static List<Row> LoadEdgesFromTriple(List<Edge<EdgeProp>> edgesTriple) {
        return edgesTriple.stream().map(triple -> {
            EdgeProp prop = triple.attr;
            if ("route".equals(prop.getLabel())) {
                return RowFactory.create(triple.srcId(), triple.dstId(), prop.getLabel(), prop.getDist());
            } else {
                return RowFactory.create(triple.srcId(), triple.dstId(), prop.getLabel(), null);
            }

        }).collect(Collectors.toList());
    }
}
