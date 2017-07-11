--Najdete zeme, ktere nemaji zadne lokace
select COUNTRY_ID, c.COUNTRY_NAME
from countries c full outer join locations l using(country_id)
where location_id is null;


--Najdete zeme, ktere maji lokace bez oddeleni
select COUNTRY_ID, c.COUNTRY_NAME
from (countries c join locations l using(country_id)) full outer join departments d using(location_id)
where department_id is null;





-- 1.  Create a view called TNS containing title-name-stars triples, 
-- where the movie (title) was reviewed by a reviewer (name) and received the rating (stars). 
-- Then referencing only view TNS and table Movie, write a SQL query that returns 
-- the lastest year of any movie reviewed by Chris Jackson. You may assume movie names are unique.

create or replace view TNS as
select title, name, stars
from Movie m join Rating re using(mID) join Reviewer using(rID);

select max(year_p)
from TNS join Movie using(title)
where name = 'Chris Jackson'
group by name;


--2.  Referencing view TNS from Exercise 1 and no other tables, create a view RatingStats 
-- containing each movie title that has at least one rating, the number of ratings it received, 
-- and its average rating. Then referencing view RatingStats and no other tables, write a SQL query 
-- to find the title of the highest-average-rating movie with at least three ratings.

create or replace view RatingStats as
select title, count(stars) pocet, avg(stars) prumer
from TNS
group by title;

select title, max(prumer)
from RatingStats
where pocet>=3
group by title;


--3.  Create a view Favorites containing rID-mID pairs, where the reviewer with rID gave the movie 
-- with mID the highest rating he or she gave any movie. Then referencing only view Favorites and 
-- tables Movie and Reviewer, write a SQL query to return reviewer-reviewer-movie triples where 
-- the two (different) reviewers have the movie as their favorite. Return each pair once, i.e., don't return a pair and its inverse.

with pom as
  (select rID, mID, max(stars) from Rating group by rID, mID)
  select * from pom;

select *
from Reviewer join Rating using(rID) join Movie using (mID)
where stars >= all(select stars from Rating group by rID);


select *
from Reviewer join Rating using(rID) join Movie using (mID);



  
  
-- 4. Insert 5-star ratings by James Cameron for all movies in the database. Leave the review date as NULL. 

insert Into Rating 
select rID, mID, 5, null
from Rating
where rID in (select rID from Reviewer where name='James Cameron') and mID in (select distinct mID from Movie);
 
--from Reviewer re, Rating ra
--where name="James Cameron" and re.rID=ra.rID;


-- For all movies that have an average rating of 4 stars or higher, add 25 to the release year. (Update the existing tuples; don't insert new tuples.) 

select 

with mov_avg as
  (select mID, avg(stars) from Rating group by mID having avg(stars)>=4),
  mov_year as
  (select * from mov_avg join Movie using(mID))
  --select mID, title, year_p, director from mov_year
  update Movie set year_p = year_p+25 where mID in (select mID from mov_year);

update Movie

set year=year+25
where mID in (select mID from Rating where avg(stars)>=4);


-- Remove all ratings where the movie's year is before 1970 or after 2000, and the rating is fewer than 4 stars. 

--delete from Rating
--where exists (select *
--              from Movie m
--              where Rating.mID = m.mID and (year_p > 2000 or year_p < 1970) and stars<4);

select *
from Movie m, Rating 
where m.mID = Rating.mID and (year_p > 2000 or year_p < 1970) and stars<4;
              
