WITH ClaimDetailsEnriched AS (
    SELECT
        cm.CustomerID,
        cm.ClaimDate,
        cm.ClaimAmount,
        cm.ClaimType,
        cp.PaymentDate,
        cp.PaymentAmount,
        cp.PaymentStatus,
        pp.PremiumAmount,
        pp.PaymentFrequency,
        pp.LastPaymentDate,
        cm.PolicyStartDate,
        cm.PolicyEndDate
    FROM claims_master cm
    INNER JOIN claims_payment cp ON cm.ClaimID = cp.ClaimID
    INNER JOIN policy_premium pp ON cm.PolicyID = pp.PolicyID
),

CustomerAggregates AS (
    SELECT
        CustomerID,
        COUNT(*) AS TotalClaims,
        COUNT(CASE WHEN PaymentStatus = 'Paid' THEN 1 END) AS TotalPaidClaims,
        COUNT(CASE WHEN PaymentStatus = 'Rejected' THEN 1 END) AS TotalRejectedClaims,
        COUNT(CASE WHEN PaymentStatus = 'Pending' THEN 1 END) AS TotalPendingClaims,
        SUM(ClaimAmount) AS TotalClaimAmount,
        AVG(ClaimAmount) AS AvgClaimAmount,
        MAX(ClaimDate) AS LastClaimDate,
        MIN(ClaimDate) AS FirstClaimDate,
        AVG(PremiumAmount) AS AvgPremiumAmount,
        COUNT(DISTINCT ClaimType) AS DistinctClaimTypes,
        (MAX(ClaimDate) - MIN(ClaimDate)) AS DaysBetweenFirstAndLastClaim,
        (MAX(ClaimDate) - MIN(PolicyStartDate)) AS DaysSincePolicyStartToLastClaim,
        (MAX(PolicyEndDate) - MIN(PolicyStartDate)) AS PolicyDurationInDays
    FROM ClaimDetailsEnriched
    GROUP BY CustomerID
)

SELECT
    ca.*,
    (ca.TotalRejectedClaims * 1.0 / NULLIF(ca.TotalClaims, 0))::NUMERIC(10,2) AS RejectedClaimRatio,
    (ca.TotalPaidClaims * 1.0 / NULLIF(ca.TotalClaims, 0))::NUMERIC(10,2) AS PaidClaimRatio,
    (ca.TotalPendingClaims * 1.0 / NULLIF(ca.TotalClaims, 0))::NUMERIC(10,2) AS PendingClaimRatio
FROM CustomerAggregates ca
ORDER BY CustomerID;