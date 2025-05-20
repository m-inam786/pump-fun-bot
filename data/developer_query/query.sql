WITH all_tokens_for_rugger_check AS (
  -- Get all tokens from both tables for rugger detection
  SELECT owner, created FROM stagnant_mints
  UNION ALL
  SELECT owner, created FROM mints
),
recent_twenty_coins AS (
  SELECT
    owner AS dev_address,
    created,
    ROW_NUMBER() OVER (
      PARTITION BY owner 
      ORDER BY created DESC
    ) AS rn
  FROM all_tokens_for_rugger_check
),
last_twenty AS (
  SELECT *
  FROM recent_twenty_coins
  WHERE rn <= 20
),
coin_time_gaps AS (
  SELECT
    dev_address,
    created,
    LAG(created) OVER (
      PARTITION BY dev_address 
      ORDER BY created ASC
    ) AS prev_created,
    created - LAG(created) OVER (
      PARTITION BY dev_address 
      ORDER BY created ASC
    ) AS time_gap_seconds
  FROM last_twenty
),
potential_ruggers AS (
  SELECT DISTINCT dev_address
  FROM coin_time_gaps
  WHERE time_gap_seconds < 600  -- 10 minutes = 600 seconds
    AND time_gap_seconds IS NOT NULL
),
-- Find the most recent token for each developer across both tables
all_developer_tokens AS (
  SELECT owner, created, 'active' as status FROM mints
  UNION ALL
  SELECT owner, created, 'stagnant' as status FROM stagnant_mints
),
most_recent_token AS (
  SELECT 
    owner AS dev_address,
    status,
    ROW_NUMBER() OVER (PARTITION BY owner ORDER BY created DESC) as rn
  FROM all_developer_tokens
),
-- Developers whose MOST RECENT token is still active (missed opportunity)
developers_with_recent_active AS (
  SELECT DISTINCT dev_address
  FROM most_recent_token
  WHERE rn = 1 AND status = 'active'
),
-- For the main analysis, get up to 3 most recent coins per developer
latest_coins AS (
  SELECT
    owner AS dev_address,
    mint_id,
    tx_counts,
    final_cumulative_volume,
    final_market_cap,
    created,
    timestamp,
    ROW_NUMBER() OVER (
      PARTITION BY owner 
      ORDER BY created DESC
    ) AS rn
  FROM stagnant_mints
  WHERE owner NOT IN (SELECT dev_address FROM potential_ruggers)  -- Filter out ruggers
    AND owner NOT IN (SELECT dev_address FROM developers_with_recent_active)  -- Filter out those whose LATEST mint is active
),
-- Calculate total token count for each developer to ensure they have history
dev_total_tokens AS (
  SELECT 
    dev_address,
    COUNT(*) AS total_tokens
  FROM latest_coins
  GROUP BY dev_address
),
-- Keep track of the most recent mint_id for each developer
most_recent_tokens AS (
  SELECT 
    dev_address, 
    mint_id AS most_recent_mint_id, 
    created AS latest_created,
    final_cumulative_volume,
    final_market_cap,
    tx_counts
  FROM latest_coins
  WHERE rn = 1
),
-- Only include developers whose most recent token is within past 9 days
recent_active_devs AS (
  SELECT *
  FROM most_recent_tokens
  WHERE extract(epoch from (CURRENT_TIMESTAMP - to_timestamp(latest_created))) <= 777600  -- 9 days * 24 hours * 60 minutes * 60 seconds
    AND (tx_counts::json ->> 'swaps')::int >= 150  -- Require at least 150 swaps for latest token
),
-- Count how many coins meet the volume thresholds for each developer
dev_token_counts AS (
  SELECT
    dev_address,
    COUNT(*) FILTER (WHERE final_cumulative_volume >= 40000) AS coins_above_40k,
    COUNT(*) FILTER (WHERE final_cumulative_volume >= 25000) AS coins_above_25k
  FROM latest_coins
  WHERE rn <= 3  -- Only consider up to 3 most recent coins
  GROUP BY dev_address
),
dev_classifications AS (
  -- Criterion 1: Latest coin migrated (final market cap >= 55k)
  SELECT 
    rad.dev_address,
    rad.most_recent_mint_id,
    rad.latest_created,
    rad.final_cumulative_volume,
    rad.final_market_cap,
    'Latest coin migrated (mcap ≥ 55k)' AS qualification_type,
    1 AS priority
  FROM recent_active_devs rad
  JOIN dev_total_tokens dtt ON rad.dev_address = dtt.dev_address
  WHERE rad.final_market_cap >= 55000
    AND dtt.total_tokens > 1  -- Ensure developer has token history
  
  UNION ALL
  
  -- Criterion 2: Latest coin volume above 100k
  SELECT 
    rad.dev_address,
    rad.most_recent_mint_id,
    rad.latest_created,
    rad.final_cumulative_volume,
    rad.final_market_cap,
    'Latest coin volume ≥ 100k' AS qualification_type,
    2 AS priority
  FROM recent_active_devs rad
  JOIN dev_total_tokens dtt ON rad.dev_address = dtt.dev_address
  WHERE rad.final_cumulative_volume >= 100000 and rad.final_market_cap >= 55000
  
  UNION ALL
  
  -- Criterion 3: Latest 2 coins each with volume ≥ 40k
  SELECT 
    rad.dev_address,
    rad.most_recent_mint_id,
    rad.latest_created,
    rad.final_cumulative_volume,
    rad.final_market_cap,
    'Latest 2 coins volume ≥ 40k each' AS qualification_type,
    3 AS priority
  FROM recent_active_devs rad
  JOIN dev_token_counts dtc ON rad.dev_address = dtc.dev_address
  -- Check if developer has at least 2 tokens with volume ≥ 40k each
  WHERE dtc.coins_above_40k >= 2
  
  UNION ALL
  
  -- Criterion 4: Latest 3 coins each with volume ≥ 25k
  SELECT 
    rad.dev_address,
    rad.most_recent_mint_id,
    rad.latest_created,
    rad.final_cumulative_volume,
    rad.final_market_cap,
    'Latest 3 coins volume ≥ 25k each' AS qualification_type,
    4 AS priority
  FROM recent_active_devs rad
  JOIN dev_token_counts dtc ON rad.dev_address = dtc.dev_address
  -- Check if developer has at least 3 tokens with volume ≥ 25k each
  WHERE dtc.coins_above_25k >= 3
),
ranked_classifications AS (
  SELECT
    dev_address,
    most_recent_mint_id,
    latest_created,
    final_cumulative_volume,
    final_market_cap,
    qualification_type,
    ROW_NUMBER() OVER (PARTITION BY dev_address ORDER BY priority) AS priority_rank
  FROM dev_classifications
) 
SELECT 
  dev_address,
  most_recent_mint_id,
  qualification_type,
  ROUND(final_cumulative_volume) AS volume_usd,
  ROUND(final_market_cap) AS final_mcap_usd,
  CASE
    WHEN extract(epoch from (CURRENT_TIMESTAMP - to_timestamp(latest_created))) < 60 THEN
      CONCAT(FLOOR(extract(epoch from (CURRENT_TIMESTAMP - to_timestamp(latest_created)))), ' seconds ago')
    WHEN extract(epoch from (CURRENT_TIMESTAMP - to_timestamp(latest_created))) < 3600 THEN
      CONCAT(FLOOR(extract(epoch from (CURRENT_TIMESTAMP - to_timestamp(latest_created))) / 60), ' min ago')
    WHEN extract(epoch from (CURRENT_TIMESTAMP - to_timestamp(latest_created))) < 86400 THEN
      CONCAT(FLOOR(extract(epoch from (CURRENT_TIMESTAMP - to_timestamp(latest_created))) / 3600), ' hours ago')
    WHEN extract(epoch from (CURRENT_TIMESTAMP - to_timestamp(latest_created))) < 259200 THEN -- Less than 3 days
      CASE
        WHEN MOD(FLOOR(extract(epoch from (CURRENT_TIMESTAMP - to_timestamp(latest_created))) / 3600), 24) = 0 THEN
          CONCAT(FLOOR(extract(epoch from (CURRENT_TIMESTAMP - to_timestamp(latest_created))) / 86400), ' days ago')
        ELSE
          CONCAT(
            FLOOR(extract(epoch from (CURRENT_TIMESTAMP - to_timestamp(latest_created))) / 86400), ' day ',
            MOD(FLOOR(extract(epoch from (CURRENT_TIMESTAMP - to_timestamp(latest_created))) / 3600), 24), 'h ago'
          )
      END
    ELSE
      CONCAT(FLOOR(extract(epoch from (CURRENT_TIMESTAMP - to_timestamp(latest_created))) / 86400), ' days ago')
  END AS time_ago
FROM ranked_classifications
WHERE priority_rank = 1  -- Take only the highest priority match for each developer
ORDER BY 
  latest_created DESC;