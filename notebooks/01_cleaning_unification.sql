-- ================================================================
-- NBA TEAM STATS — LIMPEZA E UNIFICAÇÃO
-- Fonte : nba_2022_23.csv | nba_2023_24.csv  (pasta raw/)
-- Destino: nba.silver.team_stats
--
-- Colunas geradas e usadas no notebook de análise:
--   team_name, season, ranking, games_played, wins, losses,
--   win_pct, avg_min, avg_pts, avg_fgm, avg_fga, fg_pct,
--   avg_3pm, avg_3pa, fg3_pct, avg_ftm, avg_fta, ft_pct,
--   avg_oreb, avg_dreb, avg_reb, avg_ast, avg_tov, avg_stl,
--   avg_blk, avg_blka, avg_pf, avg_pfd, plus_minus
-- ================================================================


-- ----------------------------------------------------------------
-- CÉLULA 1 — Criar schemas (não recria se já existirem)
-- ----------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS nba.bronze;
CREATE SCHEMA IF NOT EXISTS nba.silver;


-- ----------------------------------------------------------------
-- CÉLULA 2 — Ingestão raw → bronze: temporada 2022-23
-- ----------------------------------------------------------------

CREATE OR REPLACE TABLE nba.bronze.team_stats_2223
USING CSV
OPTIONS (
  path        = '/Volumes/nba/bronze/nba_files/raw/nba_2022_23.csv',
  header      = 'true',
  inferSchema = 'true'
);

SELECT '2022-23' AS temporada, COUNT(*) AS times_carregados
FROM nba.bronze.team_stats_2223;


-- ----------------------------------------------------------------
-- CÉLULA 3 — Ingestão raw → bronze: temporada 2023-24
-- ----------------------------------------------------------------

CREATE OR REPLACE TABLE nba.bronze.team_stats_2324
USING CSV
OPTIONS (
  path        = '/Volumes/nba/bronze/nba_files/raw/nba_2023_24.csv',
  header      = 'true',
  inferSchema = 'true'
);

SELECT '2023-24' AS temporada, COUNT(*) AS times_carregados
FROM nba.bronze.team_stats_2324;


-- ----------------------------------------------------------------
-- CÉLULA 4 — Limpeza e padronização: 2022-23
--
-- Mapeamento original → silver:
--   RANK   → ranking      WIN%  → win_pct     (já decimal: 0.707)
--   TEAM   → team_name    FG%   → fg_pct
--   GP     → games_played 3PM   → avg_3pm
--   W      → wins         3PA   → avg_3pa
--   L      → losses       3P%   → fg3_pct
--   MIN    → avg_min      FT%   → ft_pct
--   PTS    → avg_pts      +/-   → plus_minus
--   SEASON → season       (já no formato '2022-23')
-- ----------------------------------------------------------------

CREATE OR REPLACE VIEW nba.bronze.team_stats_2223_clean AS
SELECT
  CAST(RANK  AS INT)             AS ranking,
  TRIM(TEAM)                     AS team_name,
  CAST(GP    AS INT)             AS games_played,
  CAST(W     AS INT)             AS wins,
  CAST(L     AS INT)             AS losses,
  CAST(`WIN%` AS DOUBLE)         AS win_pct,
  CAST(MIN   AS DOUBLE)          AS avg_min,
  CAST(PTS   AS DOUBLE)          AS avg_pts,
  CAST(FGM   AS DOUBLE)          AS avg_fgm,
  CAST(FGA   AS DOUBLE)          AS avg_fga,
  CAST(`FG%` AS DOUBLE)          AS fg_pct,
  CAST(`3PM` AS DOUBLE)          AS avg_3pm,
  CAST(`3PA` AS DOUBLE)          AS avg_3pa,
  CAST(`3P%` AS DOUBLE)          AS fg3_pct,
  CAST(FTM   AS DOUBLE)          AS avg_ftm,
  CAST(FTA   AS DOUBLE)          AS avg_fta,
  CAST(`FT%` AS DOUBLE)          AS ft_pct,
  CAST(OREB  AS DOUBLE)          AS avg_oreb,
  CAST(DREB  AS DOUBLE)          AS avg_dreb,
  CAST(REB   AS DOUBLE)          AS avg_reb,
  CAST(AST   AS DOUBLE)          AS avg_ast,
  CAST(TOV   AS DOUBLE)          AS avg_tov,
  CAST(STL   AS DOUBLE)          AS avg_stl,
  CAST(BLK   AS DOUBLE)          AS avg_blk,
  CAST(BLKA  AS DOUBLE)          AS avg_blka,
  CAST(PF    AS DOUBLE)          AS avg_pf,
  CAST(PFD   AS DOUBLE)          AS avg_pfd,
  CAST(`+/-` AS DOUBLE)          AS plus_minus,
  TRIM(SEASON)                   AS season
FROM nba.bronze.team_stats_2223
WHERE TEAM IS NOT NULL;


-- ----------------------------------------------------------------
-- CÉLULA 5 — Limpeza e padronização: 2023-24
-- ----------------------------------------------------------------

CREATE OR REPLACE VIEW nba.bronze.team_stats_2324_clean AS
SELECT
  CAST(RANK  AS INT)             AS ranking,
  TRIM(TEAM)                     AS team_name,
  CAST(GP    AS INT)             AS games_played,
  CAST(W     AS INT)             AS wins,
  CAST(L     AS INT)             AS losses,
  CAST(`WIN%` AS DOUBLE)         AS win_pct,
  CAST(MIN   AS DOUBLE)          AS avg_min,
  CAST(PTS   AS DOUBLE)          AS avg_pts,
  CAST(FGM   AS DOUBLE)          AS avg_fgm,
  CAST(FGA   AS DOUBLE)          AS avg_fga,
  CAST(`FG%` AS DOUBLE)          AS fg_pct,
  CAST(`3PM` AS DOUBLE)          AS avg_3pm,
  CAST(`3PA` AS DOUBLE)          AS avg_3pa,
  CAST(`3P%` AS DOUBLE)          AS fg3_pct,
  CAST(FTM   AS DOUBLE)          AS avg_ftm,
  CAST(FTA   AS DOUBLE)          AS avg_fta,
  CAST(`FT%` AS DOUBLE)          AS ft_pct,
  CAST(OREB  AS DOUBLE)          AS avg_oreb,
  CAST(DREB  AS DOUBLE)          AS avg_dreb,
  CAST(REB   AS DOUBLE)          AS avg_reb,
  CAST(AST   AS DOUBLE)          AS avg_ast,
  CAST(TOV   AS DOUBLE)          AS avg_tov,
  CAST(STL   AS DOUBLE)          AS avg_stl,
  CAST(BLK   AS DOUBLE)          AS avg_blk,
  CAST(BLKA  AS DOUBLE)          AS avg_blka,
  CAST(PF    AS DOUBLE)          AS avg_pf,
  CAST(PFD   AS DOUBLE)          AS avg_pfd,
  CAST(`+/-` AS DOUBLE)          AS plus_minus,
  TRIM(SEASON)                   AS season
FROM nba.bronze.team_stats_2324
WHERE TEAM IS NOT NULL;


-- ----------------------------------------------------------------
-- CÉLULA 6 — Unificação bronze → silver
-- ----------------------------------------------------------------

CREATE OR REPLACE TABLE nba.silver.team_stats AS
SELECT * FROM nba.bronze.team_stats_2223_clean
UNION ALL
SELECT * FROM nba.bronze.team_stats_2324_clean;


-- ----------------------------------------------------------------
-- CÉLULA 7 — Validação final
-- ----------------------------------------------------------------

-- 1. Contagem por temporada — esperado: 30 por temporada
SELECT season, COUNT(*) AS times
FROM nba.silver.team_stats
GROUP BY season
ORDER BY season;

-- 2. Confirmar win_pct — deve estar entre 0 e 1
SELECT MIN(win_pct) AS min_win_pct, MAX(win_pct) AS max_win_pct
FROM nba.silver.team_stats;

-- 3. Confirmar valores de season — deve retornar só '2022-23' e '2023-24'
SELECT DISTINCT season FROM nba.silver.team_stats ORDER BY season;

-- 4. Checar nulos nas colunas usadas nas queries de análise
SELECT
  COUNT_IF(team_name  IS NULL) AS null_team_name,
  COUNT_IF(season     IS NULL) AS null_season,
  COUNT_IF(ranking    IS NULL) AS null_ranking,
  COUNT_IF(wins       IS NULL) AS null_wins,
  COUNT_IF(win_pct    IS NULL) AS null_win_pct,
  COUNT_IF(avg_pts    IS NULL) AS null_avg_pts,
  COUNT_IF(avg_ast    IS NULL) AS null_avg_ast,
  COUNT_IF(avg_tov    IS NULL) AS null_avg_tov,
  COUNT_IF(plus_minus IS NULL) AS null_plus_minus
FROM nba.silver.team_stats;

-- 5. Amostra final — deve bater com os CSVs originais
SELECT ranking, team_name, wins, win_pct, avg_pts, avg_ast, avg_tov, plus_minus, season
FROM nba.silver.team_stats
ORDER BY season, ranking
LIMIT 10;
