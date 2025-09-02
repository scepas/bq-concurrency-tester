SELECT name, SUM(number) as total
FROM `bigquery-public-data.usa_names.usa_1910_2013`
WHERE gender = 'F'
GROUP BY name
ORDER BY total DESC
LIMIT 10;
