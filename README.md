# MovieLensDataAnalysis

<b> IMDB has large datasets of all the movies released till date. This POC is intended to solve many KPIs so we can determine important insights from movie dataset.</b>
KPI 1 : - To find top 10 most viewed movies

Solution Steps : 

1. Create a mapper class for movie data (key: movie_id, value:movie_name)
2. Create a mapper class for rating data (key: movie_id, value:rating)
3. Create a join reducer class which merges rating and movie data. This reducer will write the output.(movie_name::rating_count)
4. Create a mapper class that will use intermediate output as input and find top 10 movies using TOP-N pattern.
5. Create driver class with 2 jobs. First job will be responsible to generate intermediate data and second job will use that intermediate data
   as input to final mapper. Here my second job is map only.

 yarn jar MovieLensAnalysis.jar com.kpi.mostviewed.MostViewedDriver <input-1:movie data> <input-2:rating data> <output>


KPI 2 : - Top twenty rated movies (Condition: The movie should be rated/viewed by at least 40 users)

Solution Steps : 

1. Create a mapper class for movie data (key: movie_id, value:movie_name)
2. Create a mapper class for rating data (key: movie_id, value:rating)
3. Create a join reducer class which merges rating and movie data. This reducer will write the intermediate output by validating rating list size is at least 40. (movie_name::rating_average)
4. Create a mapper class that will use intermediate output as input and find top 40 movies using TOP-N pattern.
5. Create driver class with 2 jobs. First job will be responsible to generate intermediate data and second job will use that intermediate data
   as input to final mapper. Here my second job is map only.
   
 yarn jar MovieLensAnalysis.jar com.kpi.toprated.TopRatedDriver <input-1:movie data> <input-2:rating data> <output>

KPI 3 : - Genres ranked by Average Rating, for each profession and age group. The age groups to be considered are: 18-35, 36-50 and 50+.

Solution Steps : 
1. Create a hashmap for age data and profession data.
2. Create a mapper class for user data (key: user_id, value:age,profession) 
3. Create a mapper class for rating data (key: user_id, value:movie_id,rating)
4. Create a join reducer class that joins user and rating data (age group::profession::movie_id::rating)
5. Create a mapper class for movie data (key: movie_id, value:genre)
6. Create a join reducer class which merges movie data and generated output from step 4. This reducer will write genre wise data. (genre::age group::profession::rating)
7. Create a mapper class that will use step 6 output. (key: age group+profession, value : genre+rating)
8. Create a reducer class that will find average rating for particular genre and write it in decreasing order of average. (age group::profession::genre1|genre2|genre3)

9. Create a driver class with 3 jobs. First job will use user and rating mapper. Second job will use joined user and rating data along with movie data mapper. Final job will rank generes and write the final output in HDFS.


   
 yarn jar MovieLensAnalysis.jar com.kpi.topgrnre.GenreRankingDriver <input-1:user data> <input-2:rating data> <input-3:movie data> <output>
