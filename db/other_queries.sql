-- Used to identify the ideal 4 buckers for premium amount
WITH Percentiles AS (
    SELECT 
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY PremiumAmount) AS P25,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY PremiumAmount) AS P50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY PremiumAmount) AS P75
    FROM 
        gms.policy_premium
),
BucketedData AS (
    SELECT 
        pp.*,
        CASE
            WHEN PremiumAmount <= (SELECT P25 FROM Percentiles) THEN 'Q1: 0-25%'
            WHEN PremiumAmount <= (SELECT P50 FROM Percentiles) THEN 'Q2: 26-50%'
            WHEN PremiumAmount <= (SELECT P75 FROM Percentiles) THEN 'Q3: 51-75%'
            ELSE 'Q4: 76-100%'
        END AS PremiumBucket
    FROM 
        gms.policy_premium pp
)
SELECT 
    PremiumBucket,
    COUNT(*) AS PolicyCount,
    AVG(PremiumAmount) AS AvgPremium,
    MIN(PremiumAmount) AS MinPremium,
    MAX(PremiumAmount) AS MaxPremium
FROM 
    BucketedData
GROUP BY 
    PremiumBucket
ORDER BY 
    PremiumBucket;


-- Manually assign ranges for buckets for premium amounts
SELECT
    CASE
        WHEN PremiumAmount < 1000 THEN '0-1000'
        WHEN PremiumAmount BETWEEN 1000 AND 2500 THEN '1000-2500'
        WHEN PremiumAmount BETWEEN 2500 AND 5000 THEN '2500-5000'
        WHEN PremiumAmount >= 5000 THEN '5000+'
        ELSE 'Unknown'
    END AS PremiumBucket,
    COUNT(*) AS PolicyCount,
    AVG(PremiumAmount) AS AvgPremium,
    MIN(PremiumAmount) AS MinPremium,
    MAX(PremiumAmount) AS MaxPremium
FROM
    gms.policy_premium
GROUP BY
    PremiumBucket
ORDER BY
    PremiumBucket;