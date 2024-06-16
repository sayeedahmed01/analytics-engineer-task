WITH RecentMonthReceipts AS (
    SELECT
        receipt_id,
        EXTRACT(YEAR FROM create_date) AS year,
        EXTRACT(MONTH FROM create_date) AS month
    FROM Fact_Receipts
    WHERE create_date >= date_trunc('month', current_date) - INTERVAL '1 month'
)
SELECT
    b.brand_name,
    COUNT(r.receipt_id) AS receipt_count
FROM
    RecentMonthReceipts r
        JOIN Dim_Receipt_Items i ON r.receipt_id = i.receipt_id
        JOIN Dim_Brands b ON i.brand_code = b.brand_code
GROUP BY
    b.brand_name
ORDER BY
    receipt_count DESC
LIMIT 5;