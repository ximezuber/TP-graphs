package ar.edu.itba.graph.model;

import java.io.Serializable;

public class EdgeProp implements Serializable {
    private final String label;
    private final Integer dist;

    private EdgeProp(String label, Integer dist) {
        this.label = label;
        this.dist = dist;
    }

    public static EdgeProp createRoute(String label, int dist) {
        return new EdgeProp(label, dist);
    }

    public static EdgeProp createContains(String label) {
        return new EdgeProp(label, null);
    }
    public String getLabel() {
        return label;
    }

    public int getDist() {
        return dist;
    }

    @Override
    public String toString() {
        String string = "EdgeProp {" +
                "label = '" + label + '\'';
        if (dist != null) string += ", dist = " + dist;
        string += '}';
        return string;
    }
}
