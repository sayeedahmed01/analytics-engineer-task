WITH RecentMonthReceipts AS (
    SELECT
        receipt_id,
        EXTRACT(YEAR FROM create_date) AS year,
        EXTRACT(MONTH FROM create_date) AS month
    FROM Fact_Receipts
    WHERE create_date >= date_trunc('month', current_date) - INTERVAL '1 month'
),
     PreviousMonthReceipts AS (
         SELECT
             receipt_id,
             EXTRACT(YEAR FROM create_date) AS year,
             EXTRACT(MONTH FROM create_date) AS month
         FROM Fact_Receipts
         WHERE create_date >= date_trunc('month', current_date) - INTERVAL '2 month'
           AND create_date < date_trunc('month', current_date) - INTERVAL '1 month'
     )
SELECT
    b.brand_name,
    COUNT(rm.receipt_id) AS recent_month_receipts,
    COUNT(pm.receipt_id) AS previous_month_receipts
FROM
    Dim_Receipt_Items i
        JOIN Dim_Brands b ON i.brand_code = b.brand_code
        LEFT JOIN RecentMonthReceipts rm ON i.receipt_id = rm.receipt_id
        LEFT JOIN PreviousMonthReceipts pm ON i.receipt_id = pm.receipt_id
GROUP BY
    b.brand_name
ORDER BY
    recent_month_receipts DESC, previous_month_receipts DESC
LIMIT 5;
