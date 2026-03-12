# check out the current wal level => SHOW wal_level;

# configure way level to logical => 
ALTER SYSTEM SET wal_level = 'logical';


# reset was level =>
ALTER SYSTEM RESET wal_level;

#tables created

# table to deposit data for successfully approved loans off delta lake gold layer 
CREATE TABLE IF NOT EXISTS success_reviewed_table_three (id SERIAL PRIMARY KEY ,
                                                    name TEXT,
                                                    operation TEXT,
                                                    profileid INTEGER ,
                                                    age TEXT, 
                                                    assessment TEXT, 
                                                    target_amount TEXT, 
                                                    payment_completion_timeline TEXT,
                                                    residential_location TEXT,
                                                    event_time TIMESTAMP WITH TIME ZONE,
                                                    no_of_kids INTEGER, 
                                                    marital_status TEXT,
                                                    no_of_cars INTEGER,
                                                    no_of_houses INTEGER,    
                                                    occupation_type TEXT,
                                                    average_monthly_income INTEGER,
                                                    average_monthly_spend INTEGER,
                                                    business_valuation INTEGER,
                                                    number_of_staffs INTEGER,
                                                    mobility_index INTEGER,
                                                    gender TEXT,
                                                    review_round TEXT,
                                                    applied_target_amount BIGINT,
                                                    loan_review TEXT,
                                                    business_loan_review TEXT
                                                    
)



# table to deposit data for failed approval of loans off delta lake gold layer
CREATE TABLE IF NOT EXISTS failure_reviewed_table_three (id SERIAL PRIMARY KEY ,
                                                    name TEXT,
                                                    operation TEXT,
                                                    profileid INTEGER ,
                                                    age TEXT, 
                                                    assessment TEXT, 
                                                    target_amount TEXT, 
                                                    payment_completion_timeline TEXT,
                                                    residential_location TEXT,
                                                    event_time TIMESTAMP WITH TIME ZONE,
                                                    no_of_kids INTEGER, 
                                                    marital_status TEXT,
                                                    no_of_cars INTEGER,
                                                    no_of_houses INTEGER,
                                                    occupation_type TEXT,
                                                    average_monthly_income INTEGER,
                                                    average_monthly_spend INTEGER,
                                                    business_valuation INTEGER,
                                                    number_of_staffs INTEGER,
                                                    mobility_index INTEGER,
                                                    gender TEXT,
                                                    review_round TEXT,
                                                    applied_target_amount BIGINT,
                                                    loan_review TEXT,
                                                    business_loan_review TEXT
)

# table to deposit data for successfully approved loans off delta lake gold layer from super-imposed model targeted at business owners demographics
CREATE TABLE IF NOT EXISTS failure_reviewed_table_three (id SERIAL PRIMARY KEY ,
                                                    name TEXT,
                                                    operation TEXT,
                                                    profileid INTEGER ,
                                                    age TEXT, 
                                                    assessment TEXT, 
                                                    target_amount TEXT, 
                                                    payment_completion_timeline TEXT,
                                                    residential_location TEXT,
                                                    event_time TIMESTAMP WITH TIME ZONE,
                                                    no_of_kids INTEGER, 
                                                    marital_status TEXT,
                                                    no_of_cars INTEGER,
                                                    no_of_houses INTEGER,
                                                    occupation_type TEXT,
                                                    average_monthly_income INTEGER,
                                                    average_monthly_spend INTEGER,
                                                    business_valuation INTEGER,
                                                    number_of_staffs INTEGER,
                                                    mobility_index INTEGER,
                                                    gender TEXT,
                                                    review_round TEXT,
                                                    applied_target_amount BIGINT,
                                                    loan_review TEXT,
                                                    business_loan_review TEXT
)


# table to deposit data for every loan request, write-ahead log is captured off incremental and edited contents on this table.
CREATE TABLE IF NOT EXISTS loan_requests_three (id SERIAL PRIMARY KEY ,
                                                    name TEXT,
                                                    profileid INTEGER ,
                                                    age TEXT, 
                                                    assessment TEXT, 
                                                    target_amount TEXT, 
                                                    payment_completion_timeline TEXT,
                                                    no_of_kids INTEGER, 
                                                    marital_status TEXT,
                                                    no_of_cars INTEGER,
                                                    no_of_houses INTEGER,
                                                    residential_location TEXT,
                                                    occupation_type TEXT,
                                                    average_monthly_income INTEGER,
                                                    average_monthly_spend INTEGER,
                                                    business_valuation INTEGER,
                                                    number_of_staffs INTEGER,
                                                    mobility_index INTEGER,
                                                    gender TEXT,
                                                    review_round TEXT)


# to change the REPLICA IDENTITY of your table to FULL. This tells Postgres to log the old values of all columns in the record =>
ALTER TABLE public.loan-requests REPLICA IDENTITY FULL;

# you can check the current status =>
SELECT relname, relreplident FROM pg_class WHERE relname = 'loan_requests;
 An ‘f’ response shows all went well