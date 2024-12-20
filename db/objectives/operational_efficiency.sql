WITH ClaimProcessingTimes AS (
    SELECT
        cm.ClaimID,
        cm.ClaimType,
        cm.ClaimAmount,
        cp.PaymentDate,
        cp.PaymentStatus,
        cm.ClaimDate,
        (cp.PaymentDate - cm.ClaimDate) AS ProcessingTimeDays
    FROM claims_master cm
    INNER JOIN claims_payment cp ON cm.ClaimID = cp.ClaimID
),
OverallMetrics AS (
    SELECT
        MIN(ProcessingTimeDays) AS MinProcessingTimeDays,
        MAX(ProcessingTimeDays) AS MaxProcessingTimeDays,
        AVG(ProcessingTimeDays) AS AvgProcessingTimeDays,
        STDDEV(ProcessingTimeDays) AS StdDevProcessingTimeDays,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ProcessingTimeDays) AS MedianProcessingTimeDays
    FROM ClaimProcessingTimes
)

SELECT
    cpt.*,
    om.MinProcessingTimeDays,
    om.MaxProcessingTimeDays,
    om.AvgProcessingTimeDays,
    om.MedianProcessingTimeDays,
    om.StdDevProcessingTimeDays
FROM ClaimProcessingTimes cpt
CROSS JOIN OverallMetrics om
ORDER BY cpt.ProcessingTimeDays DESC;