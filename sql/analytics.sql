-- Revenue by country
SELECT c.Country, SUM(f.total_amount) revenue
FROM fact_sales f
JOIN dim_customer c ON f.CustomerID = c.CustomerID
GROUP BY c.Country
ORDER BY revenue DESC;

-- Monthly revenue
SELECT
  strftime('%Y-%m', InvoiceDate) AS month,
  SUM(total_amount) revenue
FROM fact_sales
GROUP BY month;
