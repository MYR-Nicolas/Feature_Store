-- Working Copy
CREATE TABLE stock_prices_work AS
SELECT *
FROM stock_prices;

-- type control 
SELECT
    column_name,
    data_type
FROM information_schema.columns
WHERE table_name = 'stock_prices_work';

-- Missing Value Validation
SELECT ts, open_price, high_price,
	   low_price, close_price, volume
FROM stock_prices_work
WHERE ts IS NULL OR
	open_price IS NULL OR
	high_price IS NULL OR
	low_price IS NULL OR
	close_price IS NULL OR
	volume IS NULL ;
	
-- Duplicate Value Validation
SELECT ts
FROM stock_prices_work
GROUP BY ts
HAVING COUNT(*) > 1;

-- Symbol 
SELECT DISTINCT symbol 
FROM stock_prices_work;


-- Period Start and End

SELECT min(ts) as "début de la période", 
	   max(ts) as "fin de la période"
FROM stock_prices_work;
 

-- Number of Observations
SELECT COUNT(ts)
FROM stock_prices_work;

-- Monthly Aggregation for Distribution Visualization

SELECT
    date_trunc('month', ts) AS month,
    ROUND (AVG(open_price),2)  AS avg_open,
    ROUND (AVG(high_price),2)  AS avg_high,
    ROUND(AVG(low_price),2)   AS avg_low,
    ROUND (AVG(close_price),2) AS avg_close,
    ROUND (AVG(volume) ,2)     AS avg_volume
FROM stock_prices
GROUP BY date_trunc('month', ts)
ORDER BY month;

-- Yearly Aggregation for Distribution Visualization
SELECT
    date_trunc('year', ts) AS year,
    ROUND (AVG(open_price),2)  AS avg_open,
	ROUND (AVG(volume) ,2) AS avg_volume
FROM stock_prices
GROUP BY date_trunc('year', ts)
ORDER BY year;




