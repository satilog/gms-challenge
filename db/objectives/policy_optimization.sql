WITH PolicyCharacteristics AS (
    SELECT
        cm.Region,  
        cm.CustomerGender, 
        CASE
            WHEN cm.CustomerAge BETWEEN 0 AND 18 THEN '0-18'
            WHEN cm.CustomerAge BETWEEN 19 AND 30 THEN '19-30'
            WHEN cm.CustomerAge BETWEEN 31 AND 50 THEN '31-50'
            WHEN cm.CustomerAge BETWEEN 51 AND 65 THEN '51-65'
            ELSE '65+'
        END AS CustomerAgeGroup,
        pp.PaymentFrequency, 
        (cm.PolicyEndDate - cm.PolicyStartDate)::INT AS PolicyDurationDays,  
        cm.ClaimAmount,  
        pp.PremiumAmount  
    FROM claims_master cm
    INNER JOIN policy_premium pp ON cm.PolicyID = pp.PolicyID
),

BucketedPolicyCharacteristics AS (
    SELECT
        *,
        CASE
            WHEN PremiumAmount < 1000 THEN '0-1000'
            WHEN PremiumAmount BETWEEN 1000 AND 2500 THEN '1000-2500'
            WHEN PremiumAmount BETWEEN 2500 AND 5000 THEN '2500-5000'
            ELSE '5000+'
        END AS PremiumAmountClass, 
        CASE
            WHEN PolicyDurationDays < 365 THEN 'Less than a year'
            WHEN PolicyDurationDays BETWEEN 365 AND 730 THEN '1-2 years'
            WHEN PolicyDurationDays BETWEEN 730 AND 1095 THEN '2-3 years'
            ELSE '3+ years'
        END AS PolicyDurationBucket 
    FROM PolicyCharacteristics
),

FinalAggregation AS (
    SELECT
        Region,
        CustomerAgeGroup,
        CustomerGender,
        PolicyDurationBucket,
        PremiumAmountClass,
        PaymentFrequency,
        COUNT(*) AS NumberOfPolicies,
        SUM(ClaimAmount) AS TotalClaimAmount,
        AVG(ClaimAmount) AS AverageClaimAmount,
        AVG(PremiumAmount) AS AveragePremium,
        AVG(PolicyDurationDays) AS AveragePolicyDuration
    FROM BucketedPolicyCharacteristics
    GROUP BY Region, CustomerAgeGroup, CustomerGender, PolicyDurationBucket, PremiumAmountClass, PaymentFrequency
)

SELECT
    pg.*,
    (pg.AverageClaimAmount * 1.0 / NULLIF(pg.AveragePremium, 0))::NUMERIC(10,2) AS ClaimToPremiumRatio
FROM FinalAggregation pg
ORDER BY pg.PremiumAmountClass, pg.PolicyDurationBucket, pg.Region, pg.CustomerAgeGroup;
