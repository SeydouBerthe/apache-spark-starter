package ma.seydou.worstmovie.rdd;

import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

/**
 * @author : Seydou BERTHE
 *
 *
 */

public class IMDBWorstMovie {

    public static void main(String... args){
        System.setProperty("hadoop.home.dir", "D:\\BIG_DATA_WS\\lib");

        SparkConf sparkConf = new SparkConf()
                .setAppName("LowestRated Movies From ImDB")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        Map<Integer, Movie> movieById = loadMovies(sc).keyBy(Movie::getId).collectAsMap();

        List<MovieSummary> topLowestRatedMovies = topNMoviesHavingNRating(sc, 10, SortType.LOWEST, 10);
        List<MovieSummary> topHighestRatedMovies = topNMoviesHavingNRating(sc, 10, SortType.HIGHEST, 100);

        System.out.println("========== Top 10 Lowest Popular Movies ==========");
        topLowestRatedMovies.forEach(mS -> System.out.println(String.format("Name : %s, AVG : %s",
                        movieById.get(mS.getId()).getName(), mS.getAverageRating())));

        System.out.println("========== Top 10 Highest Popular Movies ==========");
        topHighestRatedMovies.forEach(mS -> System.out.println(String.format("Name : %s, AVG : %s",
                movieById.get(mS.getId()).getName(), mS.getAverageRating())));
    }

    private static List<MovieSummary> topNMoviesHavingNRating(JavaSparkContext sc, int n, SortType sortType, int havingRating){

        JavaPairRDD<Integer, Iterable<MovieData>> movieDataByIdRDD = loadMovieData(sc).groupBy(MovieData::getId);

        return movieDataByIdRDD
                .filter(x -> Iterables.size(x._2()) > havingRating)
                .mapValues(IMDBWorstMovie::averageRatingOf)
                .map(x -> new MovieSummary(x._1(), x._2()))
                .sortBy(MovieSummary::getAverageRating, SortType.getSortOrderBy(sortType), 1)
                .take(n);
    }

    private static JavaRDD<Movie> loadMovies(JavaSparkContext sc){
        return sc.textFile("data/u.item").map(Movie::of);
    }

    private static JavaRDD<MovieData> loadMovieData(JavaSparkContext sc){
        return sc.textFile("data/u.data").map(MovieData::of);
    }

    private static Double averageRatingOf(Iterable<MovieData> moviesData){
        return StreamSupport.stream(moviesData.spliterator(), true)
                            .mapToDouble(MovieData::getRating)
                            .summaryStatistics()
                            .getAverage();
    }

    enum SortType {
        HIGHEST, LOWEST;
        public static boolean getSortOrderBy(SortType sortType){
            switch (sortType){
                case HIGHEST:
                    return false;
                case LOWEST:
                    return true;
                default:
                    throw new IllegalStateException();
            }
        }
    }
}
