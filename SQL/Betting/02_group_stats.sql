-- 02_group_stats.sql
-- Статистика по группам A/B

WITH player_stats AS (
    SELECT
        player_id,
        rtp_group,
        (SUM(win_amount) - SUM(bet_amount)) / SUM(bet_amount) AS roi
    FROM casino.bets
    GROUP BY player_id, rtp_group
)

SELECT
    rtp_group,
    COUNT(*) AS n,                -- количество игроков в группе
    AVG(roi) AS mean_roi,         -- средний ROI
    varSamp(roi) AS var_roi,      -- дисперсия ROI (выборочная)
    stddevSamp(roi) AS std_roi    -- стандартное отклонение ROI
FROM player_stats
GROUP BY rtp_group;
