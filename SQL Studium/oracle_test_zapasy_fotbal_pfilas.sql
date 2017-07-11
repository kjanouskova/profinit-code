INSERT INTO matches values("357",3,"3","no");
  
WITH matches_pairs AS
  (SELECT * FROM matches team1 JOIN matches team2 USING(mID) WHERE team1.tid<>team2.tid
  )
SELECT * FROM matches_pairs;

SELECT * FROM matches team1 JOIN matches team2 USING(mID) WHERE team1.tid<>team2.tid;


WITH matches_pairs AS
  (SELECT team1.tid team1_tid, team2.tid team2_tid, team1.vysl team1_vysl, team2.vysl team2_vysl, mid
  FROM matches team1 JOIN matches team2 USING(mID)
  WHERE team1.tid<>team2.tid
  ),
  bonuses AS
  (SELECT team1_tid, team2_tid, team1_vysl, team2_vysl, mid,
    CASE
      WHEN team1_vysl>team2_vysl THEN 2
      WHEN team1_vysl=team2_vysl THEN 1
      ELSE 0
    END bonus
  FROM matches_pairs
  )
SELECT team1_tid, SUM(bonus) 
FROM bonuses 
GROUP BY team1_tid;

