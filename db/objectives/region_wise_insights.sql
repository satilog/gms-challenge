WITH
-- Combine claims_master with other related tables
claims_data AS (
    SELECT
        cm.Region,
        cm.ClaimType,
        cm.ClaimID,
        cm.ClaimAmount,
        cm.PolicyStartDate,
        cm.PolicyEndDate,
        cm.CustomerAge,
        cm.CustomerGender,
        pp.PaymentFrequency,
        pp.PremiumAmount,
        cp.PaymentStatus,
        cp.PaymentAmount
    FROM
        claims_master cm
    LEFT JOIN
        claims_payment cp
    ON
        cm.ClaimID = cp.ClaimID
    LEFT JOIN
        policy_premium pp
    ON
        cm.PolicyID = pp.PolicyID
),

-- Calculate aggregated metrics
aggregated_metrics AS (
    SELECT
        Region,
        ClaimType,
        COUNT(*) AS TotalClaims,
        SUM(ClaimAmount) AS TotalClaimAmount,
        AVG(ClaimAmount) AS AvgClaimAmount,
        ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY Region)), 2) AS PercentageOfTotalClaims,
        SUM(CASE WHEN PaymentStatus = 'Pending' THEN PaymentAmount ELSE 0 END) AS TotalPendingClaimAmount,
        SUM(CASE WHEN PaymentStatus = 'Paid' THEN PaymentAmount ELSE 0 END) AS TotalPaidClaimAmount,
        SUM(CASE WHEN PaymentStatus = 'Rejected' THEN PaymentAmount ELSE 0 END) AS TotalRejectedClaimAmount,
        COUNT(CASE WHEN PaymentFrequency = 'Annual' THEN 1 END) AS NumberOfAnnualPolicySubscriptions,
        COUNT(CASE WHEN PaymentFrequency = 'Monthly' THEN 1 END) AS NumberOfMonthlyPolicySubscriptions,
        AVG(DATE_PART('year', AGE(PolicyEndDate, PolicyStartDate))) AS AveragePolicyDuration,
        SUM(CASE WHEN PaymentFrequency = 'Annual' THEN PremiumAmount ELSE PremiumAmount * 12 END) AS TotalPolicyPremiumAmount,
        AVG(CustomerAge) AS AverageAgeOfCustomer,
        COUNT(CASE WHEN CustomerGender = 'Male' THEN 1 END) AS NoOfMaleCustomers,
        COUNT(CASE WHEN CustomerGender = 'Female' THEN 1 END) AS NoOfFemaleCustomers
    FROM
        claims_data
    GROUP BY
        Region, ClaimType
)

-- Final output
SELECT
    Region,
    ClaimType,
    TotalClaims,
    TotalClaimAmount,
    AvgClaimAmount,
    PercentageOfTotalClaims,
    TotalPendingClaimAmount,
    TotalPaidClaimAmount,
    TotalRejectedClaimAmount,
    NumberOfAnnualPolicySubscriptions,
    NumberOfMonthlyPolicySubscriptions,
    AveragePolicyDuration,
    TotalPolicyPremiumAmount,
    AverageAgeOfCustomer,
    NoOfMaleCustomers,
    NoOfFemaleCustomers
FROM
    aggregated_metrics
ORDER BY
    Region, TotalClaimAmount DESC;