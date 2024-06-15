SELECT
    CASE
        WHEN rewards_receipt_status = 'FINISHED' THEN 'ACCEPTED'
        ELSE rewards_receipt_status
        END AS rewards_receipt_status,
    AVG(total_spent) AS average_spend
FROM Fact_Receipts
WHERE rewards_receipt_status IN ('FINISHED', 'REJECTED')
GROUP BY rewards_receipt_status;
