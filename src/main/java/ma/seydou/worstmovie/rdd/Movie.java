package ma.seydou.worstmovie.rdd;

import java.io.Serializable;
import java.util.regex.Pattern;

public class Movie implements Serializable{

    private Integer id;
    private String name;
    private Double rating;

    public static Movie of(String pipeSeparatedMovie){
        String[] row = pipeSeparatedMovie.split(Pattern.quote("|"));
        return new Movie(Integer.parseInt(row[0]), row[1]);
    }

    private Movie(Integer id, String name){
        this.id = id;
        this.name = name;
    }

    public Movie(Integer id, Double rating){
        this.id = id;
        this.rating = rating;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getRating() {
        return rating;
    }

    public void setRating(Double rating) {
        this.rating = rating;
    }

    @Override
    public String toString() {
        return name;
    }
}
