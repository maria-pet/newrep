-- public.main_agg_analytics source

CREATE OR REPLACE VIEW public.main_agg_analytics
AS WITH teams_results AS (
         SELECT ffmr.season,
            ffmr.mdate,
            ffmr.home,
            ffmr.away,
            ffmr.home_sc::integer AS home_sc,
            ffmr.away_sc::integer AS away_sc,
            fmr.home_val,
            fmr.away_val
           FROM fct_full_match_results ffmr
             LEFT JOIN fct_match_rait fmr ON ffmr.season::text = fmr.season::text AND ffmr.home::text = fmr.home::text AND ffmr.away::text = fmr.away::text
        ), raw_piv_tb AS (
         SELECT t1.season,
            to_date("substring"(t1.mdate::text, 1, 10), 'yyyy-mm-dd'::text) AS m_date,
                CASE
                    WHEN t1.home::text = t2.team_name THEN t1.home
                    ELSE t1.away
                END AS team,
                CASE
                    WHEN t1.home::text = t2.team_name THEN t1.away
                    ELSE t1.home
                END AS enemy,
                CASE
                    WHEN t1.home::text = t2.team_name THEN t1.home_sc
                    ELSE t1.away_sc
                END AS team_sc,
                CASE
                    WHEN t1.home::text = t2.team_name THEN t1.away_sc
                    ELSE t1.home_sc
                END AS enemy_sc,
                CASE
                    WHEN t1.home::text = t2.team_name THEN t1.home_val
                    ELSE t1.away_val
                END AS team_val,
                CASE
                    WHEN t1.home::text = t2.team_name THEN t1.away_val
                    ELSE t1.home_val
                END AS enemy_val
           FROM teams t2
             JOIN teams_results t1 ON t1.home::text = t2.team_name OR t1.away::text = t2.team_name
        )
 SELECT row_number() OVER (PARTITION BY rpt.team, rpt.season ORDER BY rpt.m_date) AS r_n,
    rpt.season,
    rpt.m_date,
    rpt.team,
    rpt.enemy,
    rpt.team_sc,
    rpt.enemy_sc,
    rpt.team_val,
    rpt.enemy_val,
    avg(rpt.team_sc) OVER (PARTITION BY rpt.team, rpt.season ORDER BY rpt.m_date ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)::numeric(3,1) AS m3_team_sc,
    avg(rpt.enemy_sc) OVER (PARTITION BY rpt.team, rpt.season ORDER BY rpt.m_date ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)::numeric(3,1) AS m3_enemy_sc,
    avg(rpt.team_val) OVER (PARTITION BY rpt.team, rpt.season ORDER BY rpt.m_date ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)::numeric(3,2) AS m3_team_val,
    avg(rpt.team_sc) OVER (PARTITION BY rpt.team, rpt.season ORDER BY rpt.m_date)::numeric(3,1) AS avg_team_sc,
    avg(rpt.enemy_sc) OVER (PARTITION BY rpt.team, rpt.season ORDER BY rpt.m_date)::numeric(3,1) AS avg_team_miss,
    avg(rpt.team_val) OVER (PARTITION BY rpt.team, rpt.season ORDER BY rpt.m_date)::numeric(3,2) AS avg_team_val,
    rpt.team_sc - rpt.enemy_sc AS team_gl_diff
   FROM raw_piv_tb rpt
  ORDER BY rpt.team, rpt.m_date;