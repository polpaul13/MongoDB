use aueb

// Part A Question 1

  db.football.aggregate(
   {$project: { _id: 0, rounds: 1}},
   {$unwind: "$rounds"},
   {$unwind: "$rounds.matches"},
    {$match: { '$and': [   
         {"rounds.matches.team1.name" : "Leicester City"}, 
         {"rounds.matches.team2.name" : "Norwich"} // condition home and away team 
     ] } },
     
     {$project : {"_id" : "Leicester vs Norwich", Leicester : "$rounds.matches.score1", Norwich : "$rounds.matches.score2"}},
     {$out: "AQ1"});

// Part A Question 2

db.football.aggregate(
   {$project: { _id:0 , rounds: 1}},
   {$unwind: "$rounds"},
   {$unwind: "$rounds.matches"},
   {$match: { '$or' : 
                     [{'$and': [
                                {"rounds.matches.team1.name" : "Manchester City"}, 
                                {"rounds.matches.team2.name" : "Liverpool"}]},  // condition for the first match (home team = manchester city)
                      {'$and': [
                                {"rounds.matches.team1.name" : "Liverpool"}, 
                                {"rounds.matches.team2.name" : "Manchester City"}]} ]}},  //condition for the second match (home team = liverpool)
   {$project : {"_id" :  "ManchesterCity_and__Liverpool_played_at" , Date : "$rounds.matches.date"}},
   {$group : { _id : "$_id",
            Dates : { $push :"$Date"  }}}, // use push to get rsults in the same array
   {$out : "AQ2"});
   

// Part A Question 3
     
 db.football.aggregate([   {$unwind: "$rounds"},
                           {$unwind: "$rounds.matches"},
                           {$group : {
                                        "_id": "$rounds.matches.team2.name",
       			                        Team: {$max : "$rounds.matches.team2.name"},       				
                                        AwayWins: {$sum: 
                    			        {$cond : [
                    				    {$gt: ["$rounds.matches.score2","$rounds.matches.score1"]},1,0]}}}}, // use $cond to find if the away team won or not , if yes then sum increases by 1
                           {$group : { 
                                        "_id" : "$AwayWins", // group by number of away wins to get all teams in case more than 1 teams have won the same away games  
                                        Teams : { $push :"$Team"}}}, // use push get all teams in the same array
                           {$sort :  { _id : -1 }}, // sort decenting
                           {$limit : 1 } , // kepp only the teams with the most away wins
                           {$project : {"_id" : 0, AwayWins : "$_id", Teams : "$Teams"}},
                           {$out : "AQ3"}]);    
   
 
// Part A Question 4 
 
db.football.aggregate([
           {$facet: {  // use facet to have multiple aggregation pipelines
           "Home_Results":[  // first bucket includes home results for each team
           {$unwind: "$rounds"},
           {$unwind: "$rounds.matches"},
           {$group: {
                 "_id": "$rounds.matches.team1.name",
       			  HomeTeam: {$max : "$rounds.matches.team1.name"},       				
                  Home_Win_Points: {$sum: 
                    			  {$cond : [
                    		      {$gt: [ "$rounds.matches.score1", "$rounds.matches.score2"] },3, 0 ]}},
                  Home_Draw_Points:{$sum: 
                    			  {$cond : [
                    			  {$eq: [ "$rounds.matches.score1", "$rounds.matches.score2"] },1, 0 ]}},
                 
                  Home_Goals_Scored:   {$sum: "$rounds.matches.score1"},                  
                  Home_Goals_Against: {$sum: "$rounds.matches.score2"}, 			                 
                  Home_Goal_Difference:      { $sum : {$subtract: [ "$rounds.matches.score1", "$rounds.matches.score2"]}}}}],
  
           "Away_Results":[ // second bucket includes away results for each team
          {$unwind: "$rounds"},
          {$unwind: "$rounds.matches"},
          {$group: {
                 "_id": "$rounds.matches.team2.name", 
                  AwayTeam:       {$max : "$rounds.matches.team2.name"},                  
                  Away_Win_Points: {$sum: 
                    			  {$cond : [
                    			  {$gt: [ "$rounds.matches.score2", "$rounds.matches.score1"] },3, 0 ]}},  
				  Away_Draw_Points:{$sum: 
                    			  {$cond : [
                    			  {$eq: ["$rounds.matches.score2","$rounds.matches.score1"]},1,0]}},                 
                  Away_Goals_Scored:   {$sum: "$rounds.matches.score2"},                  
                  Away_Goals_Against:  {$sum: "$rounds.matches.score1"}, 			                 
                  Away_Goal_Difference:{$sum : {$subtract: [ "$rounds.matches.score2", "$rounds.matches.score1"]}}}}]}},
                  
  {$project: {Results: {$concatArrays: ["$Home_Results","$Away_Results"]}}}, // use concatArrays to merge the two bucket results
  {$unwind: "$Results"},
  {$group: {
    		"_id" : "$Results._id",
    		TotalPoints:{$sum: {$sum:["$Results.Home_Win_Points","$Results.Home_Draw_Points","$Results.Away_Win_Points","$Results.Away_Draw_Points"]}},
    		TotalGoalsScored:{$sum: {$sum: ["$Results.Home_Goals_Scored","$Results.Away_Goals_Scored"]}},
    		TotalGoalsConceded:{$sum: {$sum: ["$Results.Home_Goals_Against","$Results.Away_Goals_Against"]}},
    		TotalGoalDifference:{$sum: {$sum: ["$Results.Home_Goal_Difference","$Results.Away_Goal_Difference"]}}
    		}},
  {$sort: {"TotalPoints": -1,"TotalGoalDifference": -1}}, // sort firts by total points and then by goal difference in case of ties
  {$group: {
    		 "_id": null,
    		 "Results": {$push:{
    		   				  name: "$_id",
    		   				  Points: "$TotalPoints",
    		   				  Goal_Difference: "$TotalGoalDifference",
    		   				  Goals_Scored : "$TotalGoalsScored",
							  Goals_Against: "$TotalGoalsConceded"}}}},
  {$project: {
    		"_id" : 0,
    		Results: 1}},
  {$out:  "AQ4"}]); 
  
// Part A Question 5  
 
db.football.aggregate([
           {$facet: {  // use facet to have multiple aggregation pipelines
           "Home_Results":[  // first bucket includes home results for each team
           {$unwind: "$rounds"},
           {$unwind: "$rounds.matches"},
           {$match :{"rounds.matches.date":{$lte: "2015-12-31"}}}, //extra condition for results at the end of 2015
           {$group: {
                 "_id": "$rounds.matches.team1.name",
       			  HomeTeam: {$max : "$rounds.matches.team1.name"},       				
                  Home_Win_Points: {$sum: 
                    			  {$cond : [
                    		      {$gt: [ "$rounds.matches.score1", "$rounds.matches.score2"] },3, 0 ]}},
                  Home_Draw_Points:{$sum: 
                    			  {$cond : [
                    			  {$eq: [ "$rounds.matches.score1", "$rounds.matches.score2"] },1, 0 ]}},
                 
                  Home_Goals_Scored:   {$sum: "$rounds.matches.score1"},                  
                  Home_Goals_Against: {$sum: "$rounds.matches.score2"}, 			                 
                  Home_Goal_Difference:      { $sum : {$subtract: [ "$rounds.matches.score1", "$rounds.matches.score2"]}}}}],
  
           "Away_Results":[ // second bucket includes away results for each team
          {$unwind: "$rounds"},
          {$unwind: "$rounds.matches"},
          {$match :{"rounds.matches.date":{$lte: "2015-12-31"}}},  //extra condition for results at the end of 2015
          {$group: {
                 "_id": "$rounds.matches.team2.name", 
                  AwayTeam:       {$max : "$rounds.matches.team2.name"},                  
                  Away_Win_Points: {$sum: 
                    			  {$cond : [
                    			  {$gt: [ "$rounds.matches.score2", "$rounds.matches.score1"] },3, 0 ]}},  
				  Away_Draw_Points:{$sum: 
                    			  {$cond : [
                    			  {$eq: ["$rounds.matches.score2","$rounds.matches.score1"]},1,0]}},                 
                  Away_Goals_Scored:   {$sum: "$rounds.matches.score2"},                  
                  Away_Goals_Against:  {$sum: "$rounds.matches.score1"}, 			                 
                  Away_Goal_Difference:{$sum : {$subtract: [ "$rounds.matches.score2", "$rounds.matches.score1"]}}}}]}},
                  
  {$project: {Results: {$concatArrays: ["$Home_Results","$Away_Results"]}}}, // use concatArrays to merge the two bucket results
  {$unwind: "$Results"},
  {$group: {
    		"_id" : "$Results._id",
    		TotalPoints:{$sum: {$sum:["$Results.Home_Win_Points","$Results.Home_Draw_Points","$Results.Away_Win_Points","$Results.Away_Draw_Points"]}},
    		TotalGoalsScored:{$sum: {$sum: ["$Results.Home_Goals_Scored","$Results.Away_Goals_Scored"]}},
    		TotalGoalsConceded:{$sum: {$sum: ["$Results.Home_Goals_Against","$Results.Away_Goals_Against"]}},
    		TotalGoalDifference:{$sum: {$sum: ["$Results.Home_Goal_Difference","$Results.Away_Goal_Difference"]}}
    		}},
  {$sort: {"TotalPoints": -1,"TotalGoalDifference": -1}}, // sort firts by total points and then by goal difference in case of ties
  {$group: {
    		 "_id": null,
    		 "Results": {$push:{
    		   				  name: "$_id",
    		   				  Points: "$TotalPoints",
    		   				  Goal_Difference: "$TotalGoalDifference",
    		   				  Goals_Scored : "$TotalGoalsScored",
							  Goals_Against: "$TotalGoalsConceded"}}}},
  {$project: {
    		"_id" : 0,
    		Results: 1}},
  {$out:  "AQ5"}]);  
 
 
// Part B Question 1
 
 db.mtg.aggregate ( [ 
              {$match: { $and: [ { types: { $ne: "Creature" } }, { subtypes: { $eq: "Elemental" } } ] } },
              {$project : {"_id" : "$name", Type : "$types" , Subtype : "$subtypes"}},
              {$out : "BQ1"}]);      
       
// Part B Question 2

 db.mtg.aggregate ( [
                  {$match:{"type" : "Creature — Elemental"}}, // condition card has type Creature — Elemental
                  {$group : { _id : "$power" , 
                  elementals :  { $push : { names : "$name" , colors : "$colors" }}}}, // use $push to put names and colors in the same array 
                  {$out : "BQ2"}]);          

// Part B Question 3

db.mtg.createIndex ( { color : -1 , name : 1 } )    

// Part B Question 4
   
db.mtg.aggregate ( [                    
                   {$match : { $or : [ { colors : "Green" } ,{ colors : "Blue"}]}},  //condition colors green or blue
                   {$match : { name : /^Z/ }},     // condition card starting with letter z                                                       
                   {$limit : 3 },  // select the first 3 
                   {$out : "BQ4" }]);  
                   
// Part B Question 5
                   
db.mtg.aggregate ( [ {$match : {$and : [ { colors : {$ne : null }} , { subtypes : {$ne : null }}]}}, // condition cards have both colors and subtypes
                     {$unwind : "$colors"},
                     {$unwind : "$subtypes"},
                     {$group:{"_id":{$concat:["$colors","-","$subtypes"]},
                     count : {$sum :1} }}, // use concat to get all possible combinations between colors and subtypes
                     {$out : "BQ5" }]);
 
// Part C Question 1                     
                     
db.meteorites.createIndex( { geolocation : "2dsphere" } )

// Part C Question 2  

db.runCommand(
   {
     geoNear: "meteorites",
     near: { type: "Point", coordinates: [ 37.9838, 23.7275 ] }, //  i used the coordinates of Athens
     spherical: true,
     limit : 5     
   }
);

// Part C Question 3  

  db.meteorites.aggregate ([  {$group: { "_id": { recclass : "$recclass" }, 
                             average_mass : {$avg : 	"$mass"},
                             oldest_meteorite : {$min : "$year"},
                             most_recent_meteorite :{$max : "$year"},
                             count_of_meteorites : {$sum : 1}}},
                             {$match : {"count_of_meteorites" : {$gt:5}}}, // condition recclass values appear in at least 5 meteorites
                             {$out:"CQ3"}
                             ]);
  // Part C Question 4   
  
  
  db.meteorites.update(
   { mass: { $gt: 10000 } },
   { $set: { big_one: true } }, // create the new field 
   { multi: true }  // use multi = true to update all documents
);                          
                           