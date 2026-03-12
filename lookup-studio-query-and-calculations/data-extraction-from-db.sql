#loan review round 1

SELECT 
    COALESCE(s.occupation_type, f.occupation_type, b.occupation_type) AS occupation_type,
    COALESCE(s.success_count, 0) AS success_count,
    COALESCE(f.failure_count, 0) AS failure_count,
    COALESCE(b.business_success_count, 0) AS business_success_count

FROM (
    SELECT occupation_type, COUNT(*) AS success_count
    FROM public.success_reviewed_table
    GROUP BY occupation_type
) s

FULL OUTER JOIN (
    SELECT occupation_type, COUNT(*) AS failure_count
    FROM public.failure_reviewed_table
    GROUP BY occupation_type
) f
ON s.occupation_type = f.occupation_type

FULL OUTER JOIN (
    SELECT occupation_type, COUNT(*) AS business_success_count
    FROM public.success_reviewed_business_table
    WHERE business_loan_review = 'Pass'
      AND loan_review = 'Failed'
    GROUP BY occupation_type
) b
ON COALESCE(s.occupation_type, f.occupation_type) = b.occupation_type;



#loan_review_round_2

SELECT 
    COALESCE(s.occupation_type, f.occupation_type, b.occupation_type) AS occupation_type,
    COALESCE(s.success_count, 0) AS success_count,
    COALESCE(f.failure_count, 0) AS failure_count,
    COALESCE(b.business_success_count, 0) AS business_success_count

FROM (
    SELECT occupation_type, COUNT(*) AS success_count
    FROM public.success_reviewed_table_two
    GROUP BY occupation_type
) s

FULL OUTER JOIN (
    SELECT occupation_type, COUNT(*) AS failure_count
    FROM public.failure_reviewed_table_two
    GROUP BY occupation_type
) f
ON s.occupation_type = f.occupation_type

FULL OUTER JOIN (
    SELECT occupation_type, COUNT(*) AS business_success_count
    FROM public.success_reviewed_business_table_two
    WHERE business_loan_review = 'Pass'
      AND loan_review = 'Failed'
    GROUP BY occupation_type
) b
ON COALESCE(s.occupation_type, f.occupation_type) = b.occupation_type;