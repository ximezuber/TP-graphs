package ar.edu.itba.graph;

import ar.edu.itba.graph.model.EdgeProp;
import ar.edu.itba.graph.model.VertexProp;
import ar.edu.itba.graph.utils.FileReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.reflect.ClassTag$;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class MainApp {

    public static void main(String[] args) {
        SparkConf spark = new SparkConf().setAppName("TP Final");
        JavaSparkContext sparkContext = new JavaSparkContext(spark);

        // Read file
        JavaRDD<String> lines = sparkContext.textFile(args[0]);
        List<String> file = lines.collect();

        // Turn file into InputStream
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
        List<Tuple2<Object, VertexProp>> vertices = fileReader.getVertices();
        List<Edge<EdgeProp>> edges = fileReader.getEdges();

        // Create RDDs
        JavaRDD<Tuple2<Object, VertexProp>> verticesRDD = sparkContext.parallelize(vertices);
        JavaRDD<Edge<EdgeProp>> edgesRDD = sparkContext.parallelize(edges);

        System.out.println("Vertices size = " + vertices.size());
        System.out.println("Edges size = " + edges.size());

        scala.reflect.ClassTag<VertexProp> vertexTag =
                ClassTag$.MODULE$.apply(VertexProp.class);
        scala.reflect.ClassTag<EdgeProp> edgeTag =
                ClassTag$.MODULE$.apply(EdgeProp.class);

        // Create graph
        Graph<VertexProp, EdgeProp> graph = Graph.apply(
                verticesRDD.rdd(),
                edgesRDD.rdd(),
                null,
                StorageLevel.MEMORY_ONLY(),
                StorageLevel.MEMORY_ONLY(),
                vertexTag,
                edgeTag);

        System.out.println("vertices myGraph:");
        graph.vertices().toJavaRDD().collect().forEach(System.out::println);

        System.out.println();

        System.out.println("edges myGraph:");
        graph.edges().toJavaRDD().collect().forEach(System.out::println);

    }
}
