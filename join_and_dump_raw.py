COPY (
  SELECT a.created, tl.latitude, tl.longitude, ns.name
  FROM temp_raw_newsarticles_article a
  INNER JOIN temp_raw_newsarticles_trainedcoding tc ON (tc.article_id = a.id) 
  INNER JOIN temp_raw_newsarticles_usercoding uc ON (uc.article_id = a.id) 
  INNER JOIN temp_raw_newsarticles_trainedlocation tl ON (tl.coding_id = tc.id)
  INNER JOIN temp_raw_newsarticles_newssource ns ON (ns.id = a.news_source_id)
) TO '/tmp/test.csv' DELIMITER ',' CSV HEADER ;
