package ar.edu.itba.graph.utils;

import ar.edu.itba.graph.model.EdgeProp;
import ar.edu.itba.graph.model.VertexProp;
import org.apache.spark.graphx.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLReader;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import scala.Tuple2;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FileReader {
    private final List<Tuple2<Object, VertexProp>> vertices = new ArrayList<>();
    private final List<Edge<EdgeProp>> edges = new ArrayList<>();
    private Graph graph = TinkerGraph.open();;

    public FileReader() {}

    public void read(InputStream in){
       GraphMLReader reader = GraphMLReader.build().create();
       try {
            reader.readGraph(in, graph);
       } catch (IOException e) {
           e.printStackTrace();
       }

        Iterator<Vertex> vIterator = graph.vertices();

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

       while (vIterator.hasNext()) {
           Vertex vertex = vIterator.next();
           String type = (String) vertex.property("type").value();
           String code = (String) vertex.property("code").value();
           String desc = (String) vertex.property("desc").value();
           Long id = Long.parseLong((String) vertex.id());
           VertexProp node;
           switch (type) {
               case "airport":
                   String icao = (String) vertex.property("icao").value();
                   String region = (String) vertex.property("region").value();
                   int runways = (int) vertex.property("runways").value();
                   int longest = (int) vertex.property("longest").value();
                   int elev = (int) vertex.property("elev").value();
                   String country = (String) vertex.property("country").value();
                   String city = (String) vertex.property("city").value();
                   Double lat = (Double) vertex.property("lat").value();
                   Double lon = (Double) vertex.property("lon").value();

                   node = VertexProp.createAirport(code, icao, desc, region, runways, longest, elev,
                           country, city, lat, lon);
                   vertices.add(new Tuple2<>(id, node));
                   break;
               case "version":
                   // Excluding version node

//                   String author = (String) vertex.property("author").value();
//                   String sDate = (String) vertex.property("date").value();
//                   sDate = sDate.substring(0,sDate.length() - 4);
//                   LocalDateTime date = LocalDateTime.parse(sDate, DateTimeFormatter.ofPattern ( "yyyy-MM-dd HH:mm:ss"));
//
//                   node = VertexProp.createVersion(code, date, author, desc);
//                   vertices.add(new Tuple2<>(id, node));
                   break;
               case "country":
                   node = VertexProp.createCountry(code, desc);
                   vertices.add(new Tuple2<>(id, node));
                   break;
               case "continent":
                   node = VertexProp.createContinent(code, desc);
                   vertices.add(new Tuple2<>(id, node));
                    break;
           }
       }

       Iterator<org.apache.tinkerpop.gremlin.structure.Edge> eIterator = graph.edges();

       while (eIterator.hasNext()) {
           org.apache.tinkerpop.gremlin.structure.Edge edge = eIterator.next();
           Long src = Long.parseLong((String) edge.outVertex().id());
           Long dst = Long.parseLong((String) edge.inVertex().id());
           String label = edge.label();
           if(label.equals("route")) {
               int dist = (int) edge.property("dist").value();
               EdgeProp edgeProp = EdgeProp.createRoute(label, dist);
               edges.add(new Edge<>(src, dst, edgeProp));
           } else {
               EdgeProp edgeProp = EdgeProp.createContains(label);
               edges.add(new Edge<>(src, dst, edgeProp));
           }
       }
    }

    public List<Tuple2<Object, VertexProp>> getVertices() {
        return vertices;
    }

    public List<Edge<EdgeProp>> getEdges() {
        return edges;
    }
}
