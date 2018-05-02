package ma.seydou.movierecommendation.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import java.util.Arrays;
import java.util.Map;
import java.util.regex.Pattern;

public class MovieRecommandation {

    public static void main(String... args){

        System.setProperty("hadoop.home.dir", "D:\\BIG_DATA_WS\\lib");

        SparkConf sparkConf = new SparkConf()
                .setAppName("LowestRated Movies From ImDB")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        Map<Integer, Movie> moviesById = loadMovies(sc);

        JavaRDD<Rating> trainnigDataSet = sc.textFile("data/u.data").map(data -> data.split(Pattern.quote("\t")))
                                        .map(row -> new Rating(Integer.parseInt(row[0]), Integer.parseInt(row[1]), Double.parseDouble(row[2])));
        MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(trainnigDataSet), 1, 5, 0.01);

        Rating[] recommandations = model.recommendProducts(0, 5);
        Arrays.stream(recommandations).forEach(r -> System.out.println("Movie Name : " + moviesById.get(r.product())));

    }

    private static Map<Integer, Movie> loadMovies(JavaSparkContext sc){
        return sc.textFile("data/u.item").map(Movie::of).keyBy(Movie::getId).collectAsMap();
    }
}
