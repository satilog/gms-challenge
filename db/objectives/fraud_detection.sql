WITH ClaimDetailsEnriched AS (
    SELECT
        cm.ClaimID,
        cm.CustomerID,
        cm.ClaimDate,
        cm.ClaimAmount,
        cm.ClaimType,
        cm.Region,
        cm.CustomerAge,
        cm.CustomerGender,
        cm.PolicyStartDate,
        cm.PolicyEndDate, 
        cp.PaymentDate,
        cp.PaymentAmount,
        cp.PaymentStatus,
        cd.Diagnosis,
        cd.Provider,
        cd.ServiceDate,
        pp.PremiumAmount,
        pp.PaymentFrequency,
        pp.LastPaymentDate,
        CASE
            WHEN cm.ClaimType = 'Health' AND cd.Diagnosis = 'Dental Cleaning' THEN TRUE
            WHEN cm.ClaimType = 'Travel' AND cd.Diagnosis IN ('Diabetes', 'Dental Cleaning') THEN TRUE
            WHEN cm.ClaimType = 'Dental' AND cd.Diagnosis IN ('Diabetes', 'Fracture', 'Hypertension') THEN TRUE
            ELSE FALSE
        END AS IsClaimTypeAndDiagnosisMismatch
    FROM claims_master cm
    INNER JOIN claims_payment cp ON cm.ClaimID = cp.ClaimID
    INNER JOIN claim_details cd ON cm.ClaimID = cd.ClaimID
    INNER JOIN policy_premium pp ON cm.PolicyID = pp.PolicyID
),

ClaimsWithAggregates AS (
    SELECT
        *,
        CASE
            WHEN CustomerAge BETWEEN 0 AND 18 THEN '0-18'
            WHEN CustomerAge BETWEEN 19 AND 30 THEN '19-30'
            WHEN CustomerAge BETWEEN 31 AND 50 THEN '31-50'
            WHEN CustomerAge BETWEEN 51 AND 65 THEN '51-65'
            ELSE '65+'
        END as AgeGroup,
        COALESCE(CASE WHEN PaymentFrequency = 'Monthly' THEN PremiumAmount * 12 ELSE PremiumAmount END, 0) AS AnnualPremiumAmount,
        COUNT(CASE WHEN PaymentStatus = 'Paid' THEN 1 END) OVER (PARTITION BY CustomerID) as NoOfPaymentsCompleted,
        COUNT(CASE WHEN PaymentStatus = 'Pending' THEN 1 END) OVER (PARTITION BY CustomerID) as NoOfPaymentsPending,
        COUNT(CASE WHEN PaymentStatus = 'Rejected' THEN 1 END) OVER (PARTITION BY CustomerID) as NoOfPaymentsRejected,
        (CASE WHEN ServiceDate > ClaimDate THEN TRUE ELSE FALSE END) as IsClaimDateBeforeServiceDate,
        NULLIF(PolicyEndDate::DATE - PolicyStartDate::DATE, 0) AS PolicyDurationInDays
    FROM ClaimDetailsEnriched
),

AverageClaimAmounts AS (
    SELECT
        ClaimType,
        Region,
        AVG(ClaimAmount) as AvgClaimAmount
    FROM ClaimsWithAggregates
    GROUP BY ClaimType, Region
),

AverageProviderClaimAmounts AS (
    SELECT
        Provider,
        AVG(ClaimAmount) as AvgProviderClaimAmount
    FROM ClaimsWithAggregates
    GROUP BY Provider
),

OverallAverageClaimAmount AS (
    SELECT AVG(ClaimAmount) as OverallAvgClaimAmount FROM ClaimsWithAggregates
)

SELECT
    cwa.*,
    ABS(cwa.ClaimAmount - ac.AvgClaimAmount) as ClaimAmountDiscrepancy,
    (apca.AvgProviderClaimAmount - oaca.OverallAvgClaimAmount) as ProviderClaimAmountDeviation
FROM
    ClaimsWithAggregates cwa
JOIN
    AverageClaimAmounts ac ON cwa.ClaimType = ac.ClaimType AND cwa.Region = ac.Region
INNER JOIN
    AverageProviderClaimAmounts apca ON cwa.Provider = apca.Provider
CROSS JOIN
    OverallAverageClaimAmount oaca
ORDER BY cwa.ClaimAmount DESC;