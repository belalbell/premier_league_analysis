# premier_league_analysis
## Table of contents
* [General info](#general-info)
* [Technologies](#technologies)
* [Dataset](#dataset)
* [Source Code](#sourcecode)
* [Queries](#queries)
    * [1.  The Premier League winners from 2000 to 2022.](#1--winners)
    * [2.  How many times each team won the title ?](#2--each-team-titles)
    * [3.  Which teams won the golden title (without any losses) ?](#3--golden-title)
    * [4.	Ordering the winners points from highest to lowest.](#4--winners-points)
    * [5.	How many times each team qualified to the champions league ?](#5--champions)
    * [6.	Top goals scored by each team.](#6--goals)
    * [7.	Best teams through the 22 years.](#7--best)

## General info <a name="general-info"/>
Making insights from the records of season standings in the EPL from the 2000-01 season through the 2021-22 season.
	
## Technologies <a name="technologies"/>

* **Spark** version 2.4.5
* **Scala** scala-sdk-2.11.12
* **Java** jdk-1.8.0
* **Intellij** IDEA 2022.2 (Community Edition)

## Dataset <a name="dataset"/>
The Premier League, is the top level of the English football league system. Contested by 20 clubs, it operates on a system of promotion and relegation with the English Football League. Seasons typically run from August to May with each team playing 38 matches.
This dataset contains records of season standings in the EPL from the 2000-01 season through the 2021-22 season.

The Schema of the table as follows :

  * **Season** : String   => Season of league standings
  * **Pos** : Integer     => Placement in league standings
  * **Team** : String     => Team name
  * **Pld** : Integer     => Games contested
  * **W** : Integer       => Wins
  * **D** : Integer       => Draws
  * **L** : Integer       => Losses
  * **GF** : Integer      => Goals team scored
  * **GA** : Integer      => Goals team allowed
  * **GD** : Integer      => Goal difference
  * **Pts** : Integer     => Points
  * **Qualification or relegation** : String 
  
Download the dataset at Kaggle: [English Premier League Standings](https://www.kaggle.com/datasets/evangower/english-premier-league-standings?resource=download).

## Source Code <a name="sourcecode"/>

  * **Manager.scala** : Using DataFrame and UnTyped Operation API
  * **ManagerSQL.scala** : Using DataFrame and SQL

## Queries <a name="queries"/>
#### 1.  The Premier League winners from 2000 to 2022. <a name="1--winners"/>

| Season|             Team|
|-------|-----------------|
|2000-01|Manchester United|
|2001-02|          Arsenal|
|2002-03|Manchester United|
|2003-04|          Arsenal|
|2004-05|          Chelsea|
|2005-06|          Chelsea|
|2006-07|Manchester United|
|2007-08|Manchester United|
|2008-09|Manchester United|
|2009-10|          Chelsea|
|2010-11|Manchester United|
|2011-12|  Manchester City|
|2012-13|Manchester United|
|2013-14|  Manchester City|
|2014-15|          Chelsea|
|2015-16|   Leicester City|
|2016-17|          Chelsea|
|2017-18|  Manchester City|
|2018-19|  Manchester City|
|2019-20|        Liverpool|


#### 2.  How many times each team won the title ? <a name="2--each-team-titles"/>
![pl_winners](https://user-images.githubusercontent.com/24812788/183650475-7bf1ce46-0e97-43ff-b45b-1952dcee5228.jpg)

#### 3.  Which teams won the golden title (without any losses) ? <a name="3--golden-title"/>

| Season|   Team|
|-------|-------|
|2003-04|Arsenal|


#### 4.  Ordering the winners points from highest to lowest. <a name="4--winners-points"/>

| Season|             Team|Pts|
|-------|-----------------|:-:|
|2017-18|  Manchester City|100|
|2019-20|        Liverpool| 99|
|2018-19|  Manchester City| 98|
|2004-05|          Chelsea| 95|
|2016-17|          Chelsea| 93|
|2021-22|  Manchester City| 93|
|2005-06|          Chelsea| 91|
|2008-09|Manchester United| 90|
|2003-04|          Arsenal| 90|
|2012-13|Manchester United| 89|
|2011-12|  Manchester City| 89|
|2006-07|Manchester United| 89|
|2007-08|Manchester United| 87|
|2014-15|          Chelsea| 87|
|2001-02|          Arsenal| 87|
|2013-14|  Manchester City| 86|
|2020-21|  Manchester City| 86|
|2009-10|          Chelsea| 86|
|2002-03|Manchester United| 83|
|2015-16|   Leicester City| 81|

#### 5.  How many times each team qualified to the champions league ? <a name="5--champions"/>
![qualified_to_champions](https://user-images.githubusercontent.com/24812788/183650675-154c6bcc-a19f-4e11-a2b6-72f2c3f4c661.png)



#### 6.  Top goals scored by each team. <a name="6--goals"/>

| Season|             Team|goals_per_season|
|-------|-----------------|:--------------:|
|2017-18|  Manchester City|             106|
|2009-10|          Chelsea|             103|
|2019-20|  Manchester City|             102|
|2013-14|  Manchester City|             102|
|2013-14|        Liverpool|             101|
|2021-22|  Manchester City|              99|
|2018-19|  Manchester City|              95|
|2021-22|        Liverpool|              94|
|2011-12|  Manchester City|              93|
|2018-19|        Liverpool|              89|
|2011-12|Manchester United|              89|
|2001-02|Manchester United|              87|
|2004-05|          Arsenal|              87|
|2012-13|Manchester United|              86|
|2016-17|Tottenham Hotspur|              86|
|2009-10|Manchester United|              86|
|2016-17|          Chelsea|              85|
|2019-20|        Liverpool|              85|
|2002-03|          Arsenal|              85|
|2017-18|        Liverpool|              84|

#### 7.  Best teams through the 22 years. <a name="7--best"/>

| Season|           Team|Team_score|
|-------|---------------|:--------:|
|2017-18|Manchester City|      7900|
|2018-19|Manchester City|      7056|
|2021-22|Manchester City|      6789|
|2018-19|      Liverpool|      6499|
|2021-22|      Liverpool|      6256|
|2009-10|        Chelsea|      6106|
|2011-12|Manchester City|      5696|
|2013-14|Manchester City|      5590|
|2019-20|Manchester City|      5427|
|2004-05|        Chelsea|      5415|










