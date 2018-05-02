package ma.seydou.worstmovie.rdd;

import java.io.Serializable;

public class MovieSummary implements Serializable{

    private Integer id;
    private Double averageRating;

    public MovieSummary(Integer id, Double averageRating) {
        this.id = id;
        this.averageRating = averageRating;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Double getAverageRating() {
        return averageRating;
    }

    public void setAverageRating(Double averageRating) {
        this.averageRating = averageRating;
    }
}
