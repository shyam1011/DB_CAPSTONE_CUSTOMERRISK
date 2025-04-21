#-----------------------------------
# DATA SERVING
#-----------------------------------

-- Create share
CREATE SHARE customer360_analytics;

-- Add table to share
ALTER SHARE customer360_analytics ADD TABLE gold.customer_risk_scores;

-- Add recipient (open sharing or authenticated)
CREATE RECIPIENT new_user_abc USING IDENTITY 'abc.xyz@example.com';

-- Grant access
GRANT USAGE ON SHARE customer360_analytics TO RECIPIENT partner_bank_abc;

#-----------------------------------------------------