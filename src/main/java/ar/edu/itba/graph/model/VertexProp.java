package ar.edu.itba.graph.model;

import java.io.Serializable;
import java.time.LocalDateTime;

public class VertexProp implements Serializable {
    private final String type;
    private final String code;
    private final String icao;
    private final String desc;
    private final String region;
    private final Integer runways;
    private final Integer longest;
    private final Integer elev;
    private final String country;
    private final String city;
    private final Double lat;
    private final Double lon;
    private final String author;
    private final LocalDateTime date;

    private VertexProp(String type, String code, String icao, String desc, String region,
                       Integer runways, Integer longest, Integer elev, String country, String city, Double lat,
                       Double lon, String author, LocalDateTime date) {
        this.type = type;
        this.code = code;
        this.icao = icao;
        this.desc = desc;
        this.region = region;
        this.runways = runways;
        this.longest = longest;
        this.elev = elev;
        this.country = country;
        this.city = city;
        this.lat = lat;
        this.lon = lon;
        this.author = author;
        this.date = date;
    }

    public static VertexProp createAirport(String code, String icao, String desc, String region,
                                           int runways, int longest, int elev, String country, String city, Double lat,
                                           Double lon) {
        return new VertexProp("airport", code, icao, desc, region, runways,
                longest, elev, country, city, lat, lon, null, null);
    }

    public static VertexProp createVersion(String code, LocalDateTime date, String author, String desc) {
        return new VertexProp("version", code, null, desc, null, null, null,
                null, null, null, null, null, author, date);
    }

    public static VertexProp createCountry(String code, String desc) {
        return new VertexProp("country", code, null, desc, null,null, null,
                null, null, null, null, null, null, null);
    }

    public static VertexProp createContinent(String code, String desc) {
        return new VertexProp("continent", code, null, desc, null,null, null,
                null, null, null, null, null, null, null);
    }

    public String getType() {
        return type;
    }

    public String getCode() {
        return code;
    }

    public String getIcao() {
        return icao;
    }

    public String getDesc() {
        return desc;
    }

    public String getRegion() {
        return region;
    }

    public Integer getRunways() {
        return runways;
    }

    public Integer getLongest() {
        return longest;
    }

    public Integer getElev() {
        return elev;
    }

    public String getCountry() {
        return country;
    }

    public String getCity() {
        return city;
    }

    public Double getLat() {
        return lat;
    }

    public Double getLon() {
        return lon;
    }

    public String getAuthor() {
        return author;
    }

    public LocalDateTime getDate() {
        return date;
    }

    @Override
    public String toString() {
        String string =  "VertexProp {";
        if (type != null) string += ", type='" + type + '\'';
        if (code != null) string += ", code='" + code + '\'';
        if (icao != null) string += ", icao='" + icao + '\'';
        if (desc != null) string += ", desc='" + desc + '\'';
        if (region != null) string += ", region='" + region + '\'';
        if (runways != null) string += ", runways=" + runways;
        if (longest != null) string += ", longest=" + longest;
        if (elev != null) string += ", elev=" + elev;
        if (country != null) string += ", country='" + country + '\'';
        if (city != null) string += ", city='" + city + '\'';
        if (lat != null) string += ", lat=" + lat;
        if (lon != null) string += ", lon=" + lon;
        if (author != null) string += ", author='" + author + '\'';
        if (date != null) string += ", date=" + date.toString();
        string += '}';

        return string;
    }
}
