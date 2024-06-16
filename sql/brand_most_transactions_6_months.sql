WITH RecentUsers AS (
    SELECT
        user_id
    FROM
        Dim_Users
    WHERE
        create_date >= current_date - INTERVAL '6 months'
)
SELECT
    b.brand_name,
    COUNT(r.receipt_id) AS transaction_count
FROM
    Fact_Receipts r
JOIN Dim_Receipt_Items i ON r.receipt_id = i.receipt_id
JOIN Dim_Brands b ON i.brand_code = b.brand_code
JOIN RecentUsers u ON r.user_id = u.user_id
GROUP BY
    b.brand_name
ORDER BY
    transaction_count DESC
LIMIT 1;
