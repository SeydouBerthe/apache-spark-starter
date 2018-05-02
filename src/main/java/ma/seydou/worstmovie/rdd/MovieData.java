package ma.seydou.worstmovie.rdd;

import java.io.Serializable;
import java.util.regex.Pattern;

public class MovieData implements Serializable{

    private Integer id;
    private Double rating;

    public static MovieData of(String tabSeparatedMovie){
        String[] row = tabSeparatedMovie.split(Pattern.quote("\t"));
        return new MovieData(Integer.parseInt(row[1]), Double.parseDouble(row[2]));
    }

    private MovieData(Integer id, Double rating){
        this.id = id;
        this.rating = rating;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }


    public Double getRating() {
        return rating;
    }

    public void setRating(Double rating) {
        this.rating = rating;
    }
}
