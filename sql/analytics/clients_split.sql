WITH ranked_clients AS (
    SELECT
        client_id,
        client_score,
        time_zone,
        ROW_NUMBER() OVER (
            ORDER BY client_score DESC, client_id
        ) AS rn
    FROM clients
),
assigned_splits AS (
    SELECT
        client_id,
        client_score,
        time_zone,
        ((rn - 1) % 6) + 1 AS split_id
    FROM ranked_clients
)
SELECT
    client_id,
    client_score,
    time_zone,
    split_id,
    CASE
        WHEN split_id IN (1, 4, 6) THEN TRUE
        ELSE FALSE
    END AS is_contact
FROM assigned_splits
ORDER BY
    is_contact DESC,
    client_score DESC,
    client_id;