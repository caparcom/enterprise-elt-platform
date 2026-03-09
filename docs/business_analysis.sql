-- Business questions

-- 1
-- What are the top 10 products by total revenue?

SELECT TOP 10
	total_revenue, 
	total_items_sold, 
	product_id, 
	product_name, 
	product_brand, 
	product_category, 
	product_department 
FROM gold.vw_sales_by_product
ORDER BY total_revenue DESC;

/*
Analysis:
The top revenue generating products are primarily in men's activewear and outerwear,
especially jackets. Seven of the top ten products fall into this category, indicating strong 
customer demand. A useful next step would be, to analyze whether this demand changes seasonally 
to better inform inventory planning and/or marketing campaigns.
*/



-- 2
-- How does revenue trend over time?

-- monthly? -- grain for our specific view is monthly so would need to recalculate quarterly revenue
select year, 
month_name, 
total_revenue,
CASE 
	WHEN LAG(total_revenue) OVER (ORDER BY year, month) IS NULL THEN NULL
	WHEN LAG(total_revenue) OVER (ORDER BY year, month) = 0 THEN NULL
	ELSE CAST(ROUND(((total_revenue - LAG(total_revenue) OVER (ORDER BY year, month)) 
		/ LAG(total_revenue) OVER (ORDER BY year, month)) * 100, 2) AS DECIMAL(10,2))
	END previous_month_rev_pct_change,
total_items_sold
from gold.vw_sales_by_month
order by year, month;

-- quarterly?
WITH quarterly_revenue AS (
    SELECT
        year,
        quarter,
        SUM(total_revenue) AS total_revenue,
        SUM(total_items_sold) AS total_items_sold
    FROM gold.vw_sales_by_month
    GROUP BY year, quarter
)
SELECT
    year,
    quarter,
    total_revenue,
    total_items_sold,
    CASE
        WHEN LAG(total_revenue) OVER (ORDER BY year, quarter) IS NULL THEN NULL
        WHEN LAG(total_revenue) OVER (ORDER BY year, quarter) = 0 THEN NULL
        ELSE CAST(
            ROUND(
                ((total_revenue - LAG(total_revenue) OVER (ORDER BY year, quarter))
                / LAG(total_revenue) OVER (ORDER BY year, quarter)) * 100.0,
                2
            ) AS DECIMAL(10,2)
        )
    END AS previous_quarter_rev_pct_change
FROM quarterly_revenue
ORDER BY year, quarter;

/*
Analysis:
First, clarify whether they want a monthly or quarterly view of the trend. 
My initial approach would be to start with monthly revenue to identify seasonality and 
short term fluctuations, then aggregate to quarters if they want a higher level trend 
or quarter over quarter change.
*/



