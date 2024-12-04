-- Create schema if it doesn't exist
DO
$$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'gms') THEN
        CREATE SCHEMA gms;
    END IF;
END
$$;

-- Set the schema for all subsequent table creation
SET search_path TO gms;

-- Create claims_master table
CREATE TABLE IF NOT EXISTS claims_master (
    ClaimID UUID PRIMARY KEY,
    PolicyID UUID,
    CustomerID UUID,
    ClaimDate DATE,
    ClaimAmount FLOAT,
    ClaimType TEXT,
    Region TEXT,
    CustomerAge INT,
    CustomerGender TEXT,
    PolicyStartDate DATE,
    PolicyEndDate DATE
);

-- Create claims_payment table
CREATE TABLE IF NOT EXISTS claims_payment (
    PaymentID UUID PRIMARY KEY,
    ClaimID UUID NULL, -- Allow NULL for foreign key
    PaymentDate DATE,
    PaymentAmount FLOAT,
    PaymentStatus TEXT
    -- ,
    -- FOREIGN KEY (ClaimID) REFERENCES claims_master (ClaimID)
);

-- Create policy_premium table
CREATE TABLE IF NOT EXISTS policy_premium (
    PolicyID UUID PRIMARY KEY,
    CustomerID UUID,
    PremiumAmount FLOAT,
    PaymentFrequency TEXT,
    LastPaymentDate DATE
);

-- Create claim_details table
CREATE TABLE IF NOT EXISTS claim_details (
    ClaimID UUID PRIMARY KEY,
    Diagnosis TEXT,
    ServiceDate DATE,
    Provider TEXT,
    Country TEXT
    -- ,
    -- FOREIGN KEY (ClaimID) REFERENCES claims_master (ClaimID)
);

-- Create procedure_codes table
CREATE TABLE IF NOT EXISTS procedure_codes (
    CodeID SERIAL PRIMARY KEY,
    ClaimID UUID NOT NULL,
    ProcedureCode TEXT NOT NULL
    -- ,
    -- FOREIGN KEY (ClaimID) REFERENCES claim_details (ClaimID) ON DELETE CASCADE
);