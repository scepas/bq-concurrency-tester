SELECT
    t1.name,
    t1.total,
    t2.year,
    t2.number AS number_in_year
FROM (
    SELECT name, SUM(number) as total
    FROM `bigquery-public-data.usa_names.usa_1910_2013`
    WHERE gender = 'M'
    GROUP BY name
    ORDER BY total DESC
    LIMIT 5
) AS t1
JOIN `bigquery-public-data.usa_names.usa_1910_2013` AS t2
ON t1.name = t2.name
WHERE t2.year > 2000
ORDER BY t2.year, t2.number DESC;
