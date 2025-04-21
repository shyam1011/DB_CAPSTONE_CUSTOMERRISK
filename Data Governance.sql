#Object - Level Permission
GRANT SELECT ON TABLE gold.customer_profiles TO `risk_analyst_group`;


#Schema & Catalog Level
GRANT USAGE ON SCHEMA gold TO `data_science_team`;
REVOKE ALL PRIVILEGES ON DATABASE bronze FROM PUBLIC;


#Row & Column LevelSecurity
-- Column masking for PII
CREATE FUNCTION mask_email(email STRING) RETURNS STRING RETURN CONCAT('***@', SPLIT(email, '@')[1]);
CREATE TABLE masked_customers WITH MASK (email USING mask_email(email));

-- Row filters (Preview)
CREATE FUNCTION risk_team_filter() RETURNS BOOLEAN RETURN current_user() IN ('abc.xyz@gmail.com');
CREATE ROW FILTER risk_filter ON gold.customer_risk_scores FOR ROWS WHERE risk_team_filter();



#------------------------------
#  AUDIT
#-----------------------------

-- Audit access and permission changes
SELECT timestamp, user_email, action_name, object_name
FROM system.access.audit
WHERE action_name LIKE '%GRANT%';


-- Track sensitive data queries
SELECT *
FROM system.query.history
WHERE object_name = 'customer_profiles'
AND query_text ILIKE '%ssn%';
