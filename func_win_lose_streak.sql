CREATE OR REPLACE FUNCTION public.win_lose_streak(p_date date, p_team character varying, p_season character varying)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
declare 
	v_streak integer;

begin
    WITH win AS (
         SELECT main_agg_analytics.r_n,
            main_agg_analytics.season,
            main_agg_analytics.m_date,
            main_agg_analytics.team,
            main_agg_analytics.enemy,
            main_agg_analytics.team_sc,
            main_agg_analytics.enemy_sc,
            main_agg_analytics.team_sc - main_agg_analytics.enemy_sc AS delta,
                CASE
                    WHEN (main_agg_analytics.team_sc - main_agg_analytics.enemy_sc) > 0 THEN 1
                    ELSE 0
                END AS is_win
           FROM main_agg_analytics
           where 
           m_date<p_date
           and team = p_team and season = p_season
          ORDER BY main_agg_analytics.team, main_agg_analytics.m_date
        )
 SELECT 
    case when is_win=1 then count(*) else -count(*) end AS match_num
   into v_streak
   FROM ( SELECT win.season,
            win.team,
            win.enemy,
            win.m_date,
            win.is_win,
            row_number() OVER (PARTITION BY win.season, win.team ORDER BY win.m_date) - row_number() OVER (PARTITION BY win.season, win.team, win.is_win ORDER BY win.m_date) AS diff
           FROM win
          ORDER BY win.team, win.m_date) tb
  GROUP BY tb.is_win, tb.diff, tb.team, tb.season
  ORDER BY tb.team, (max(tb.m_date)) desc 
  limit 1;
 return v_streak;
END;
$function$
;
