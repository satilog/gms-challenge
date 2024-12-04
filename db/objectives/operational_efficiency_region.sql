WITH ClaimProcessingTimes AS (
    SELECT
        cm.ClaimID,
        cm.ClaimType,
        cm.Region,
        cm.ClaimAmount,
        cp.PaymentDate,
        cp.PaymentStatus,
        cm.ClaimDate,
        (cp.PaymentDate - cm.ClaimDate) AS ProcessingTimeDays
    FROM claims_master cm
    INNER JOIN claims_payment cp ON cm.ClaimID = cp.ClaimID
),

AggregatedProcessingTimes AS (
    SELECT
        ClaimType,
        Region,
        COUNT(*) AS TotalClaims,
        AVG(ProcessingTimeDays) AS AvgProcessingTimeDays,
        MIN(ProcessingTimeDays) AS MinProcessingTimeDays,
        MAX(ProcessingTimeDays) AS MaxProcessingTimeDays,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ProcessingTimeDays) AS MedianProcessingTimeDays,
        STDDEV(ProcessingTimeDays) AS StdDevProcessingTimeDays,
        AVG(ClaimAmount) AS AvgClaimAmount
    FROM ClaimProcessingTimes
    GROUP BY ClaimType, Region
)

SELECT *
FROM AggregatedProcessingTimes
ORDER BY AvgProcessingTimeDays DESC;