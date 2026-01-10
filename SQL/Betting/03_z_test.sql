-- 03_z_test.sql
-- Z-тест: проверка, значимо ли различие ROI между группами

WITH player_stats AS (
    SELECT
        player_id,
        rtp_group,
        (SUM(win_amount) - SUM(bet_amount)) / SUM(bet_amount) AS roi
    FROM casino.bets
    GROUP BY player_id, rtp_group
),

group_stats AS (
    SELECT
        rtp_group,
        COUNT(*) AS n,
        AVG(roi) AS mean_roi,
        varSamp(roi) AS var_roi,
        stddevSamp(roi) AS std_roi
    FROM player_stats
    GROUP BY rtp_group
),

a_values AS (
    SELECT mean_roi AS mean_a, var_roi AS var_a, n AS n_a
    FROM group_stats
    WHERE rtp_group = 'A'
),

b_values AS (
    SELECT mean_roi AS mean_b, var_roi AS var_b, n AS n_b
    FROM group_stats
    WHERE rtp_group = 'B'
),

z_test AS (
    SELECT
        a.mean_a - b.mean_b AS diff_mean,                 -- разница средних ROI
        SQRT((a.var_a / a.n_a) + (b.var_b / b.n_b)) AS std_error,  -- стандартная ошибка
        (a.mean_a - b.mean_b) / SQRT((a.var_a / a.n_a) + (b.var_b / b.n_b)) AS z_value
    FROM a_values AS a
    CROSS JOIN b_values AS b
)

SELECT
    diff_mean,                                         -- разница средних ROI
    std_error,                                        -- стандартная ошибка
    z_value,                                          -- Z-статистика
    1 - erf(abs(z_value) / sqrt(2)) AS p_value       -- двусторонний p-value (приближённый)
FROM z_test;
