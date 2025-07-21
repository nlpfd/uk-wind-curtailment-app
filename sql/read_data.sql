-- 1. Joins in the `sbp` table with 'curtailment'
-- 2. Manipulates some columns which used to be manipulated in pandas (faster in SQL)
-- 3. start and end time are parameters which are interpolated by the SQL engine
-- Note a fixed gas turn up price is used #52
-- Note the gas price is 100, but in 2022 we use 200.

SELECT
    c.time                            AS time,
    level_fpn                        AS level_fpn_mw,
    level_after_boal                 AS level_after_boal_mw,
    delta_mw,
    level_fpn * 0.5                 AS level_fpn_mwh,
    level_after_boal * 0.5          AS level_after_boal_mwh,
    system_buy_price,
    cost_gbp,
    CASE
        WHEN c.time < '2022-01-01' THEN delta_mw * 0.5 * 100
        WHEN c.time >= '2022-01-01' AND c.time < '2023-01-01' THEN delta_mw * 0.5 * 200
        WHEN c.time >= '2023-01-01' THEN delta_mw * 0.5 * 100
    END AS turnup_cost_gbp
FROM public.curtailment c
LEFT JOIN public.sbp s ON c.time = s.time
WHERE c.time BETWEEN CAST(%(start_time)s AS TIMESTAMP)
    AND CAST(%(end_time)s AS TIMESTAMP)
ORDER BY c.time;
