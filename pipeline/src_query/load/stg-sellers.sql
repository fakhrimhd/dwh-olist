INSERT INTO stg.sellers 
    (seller_id, seller_zip_code_prefix, seller_city, seller_state) 

SELECT
    seller_id, 
    seller_zip_code_prefix AS seller_zip_code_prefix, 
    seller_city AS seller_city, 
    seller_state AS seller_state

FROM public.sellers

ON CONFLICT(seller_id) 
DO UPDATE SET
    seller_zip_code_prefix = EXCLUDED.seller_zip_code_prefix,
    seller_city = EXCLUDED.seller_city,
    seller_state = EXCLUDED.seller_state,
    updated_at = CASE WHEN 
                        stg.sellers.seller_city <> EXCLUDED.seller_city
                        OR stg.sellers.seller_state <> EXCLUDED.seller_state
                THEN 
                        CURRENT_TIMESTAMP
                ELSE
                        stg.sellers.updated_at
                END;
