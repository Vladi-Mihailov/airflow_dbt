-- 01_player_roi.sql
-- Расчёт ROI (Return on Investment) для каждого игрока
-- ROI = (выигрыши - ставки) / ставки

SELECT
    player_id,
    rtp_group,
    SUM(win_amount) AS total_win,       -- суммарный выигрыш игрока
    SUM(bet_amount) AS total_bet,       -- суммарная ставка игрока
    (SUM(win_amount) - SUM(bet_amount)) / SUM(bet_amount) AS roi
FROM casino.bets
GROUP BY player_id, rtp_group
ORDER BY player_id;
