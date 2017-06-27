var mysql = require('mysql');
var http = require('http');
var url = require('url');
var fs = require('fs');
var express = require('express');
var app = express();
var bodyParser = require('body-parser');
var multer = require('multer'); // v1.0.5
var apiServerPort = 8082;
endPointsDef = "<thead><tr><th>Method</th><th>Path</th><th>Object Sample</th></tr></thead>";



  
var configFile = "config.json";
var upload = multer(); // for parsing multipart/form-data

//rest API listener

var conDbHost=undefined;
var conDbPort=undefined;
var conDbDb=undefined;
var conDbSchema=undefined;
var conDbUser=undefined;
var conDbPassword=undefined;
var conDbUserTables=undefined;

var debugEnabled=true;
var tableDef=undefined;



var server = app.listen(apiServerPort, function () {

  var host = server.address().address;
  console.log("API server listening at http://%s:%s", host,apiServerPort);
  readConfig();
  

})



//supportive common functions



function readConfig(){
	fs.readFile( __dirname + "/" + "config.json", 'utf8', function (err, data) {
	   wirteToConsoleDebug( "Configuration file reading completed... " + __dirname + "/" + configFile  );
       configObject = JSON.parse(data);	   
       wirteToConsoleDebug(data);
	   conDbHost= configObject.db.dbHost;
	   conDbPort = configObject.db.dbPort;
	   conDbDb = configObject.db.db;
	   conDbSchema = configObject.db.dbSchema;
	   conDbUser = configObject.db.dbUser;
	   conDbPassword = configObject.db.dbPassword;
	   conDbUserTables = configObject.db.userTables;
	   connectToDb(conDbHost,conDbPort,conDbDb,conDbSchema,conDbUser,conDbPassword,conDbUserTables);
   });
   
   
   return;
}

//function to connec to the db
function connectToDb(conDbHost,conDbPort,conDbDb,conDbSchema,conDbUser,conDbPassword,conDbUserTables){
	var con = mysql.createConnection({
	  host: conDbHost,
	  port: conDbPort,
	  user: conDbUser,
	  password: conDbPassword,
	  database: conDbDb,
	});

	con.connect(function(err) {
	  if (err) {
		  console.log("Error connecting to the database!!!");
		  throw err;
		  
	  } else {
		console.log("Connected to the database!");
		execUseDB(con,"USE " + conDbDb,conDbUserTables);
		
	  }
	});
	
	return;
}

function execUseDB(con,sqlQry,conDbUserTables){
	//change to use the DB
	con.query(sqlQry, function (err, result) {
    if (err){
		throw err;
	} else {
	//get the table structure definition
	wirteToConsoleDebug("conDbUserTables :" + conDbUserTables);
	
	for(tbl in conDbUserTables){
		wirteToConsoleDebug("conDbUserTables Qry :" + "SHOW COLUMNS FROM " + conDbUserTables[tbl]);
		execDB(con,"SHOW COLUMNS FROM " + conDbUserTables[tbl],conDbUserTables[tbl]);
		}
	}
	
  });
  
  return;
}

function execDB(con,sqlQry,currTable){
	//get the table structure definition
	con.query(sqlQry, function (err, tableResult) {
    if (err){
		throw err;
	} else {
	wirteToConsoleDebug("result : " + JSON.stringify(tableResult));
    defineJsonModel(tableResult,con,currTable);
	}
	
  });
  
  return;
}

function defineJsonModel(tableResult,con,currTable){
	//define the sample JSON body based on table structure
	var jsonModel = "{";
	
	for( i in tableResult){
		console.log(JSON.stringify(tableResult[i]));
		var obj = JSON.parse(JSON.stringify(tableResult[i]));
		jsonModel = jsonModel + "\"" + obj.Field + "\":" + "\"" + "value of " + obj.Field + "\"" ;
		if( i < tableResult.length-1 ){
			jsonModel = jsonModel + ",";
		}
		
	}
	jsonModel = jsonModel + "}";
	wirteToConsoleDebug("JSON message sample : " + jsonModel);	
	defineEndPoints(jsonModel,currTable,tableResult,con);
	return;
}

function defineEndPoints(jsonModel,dbTable,tableResult,con){
	

	endPointsDef =  endPointsDef +  "<tr class=\"info\">"+
					"<th>GET</th>"+
					"<th>'/'"+ dbTable + "</th>" + 
					"<th>"+ jsonModel + "</th>" + 
					"</tr>";
	endPointsDef =  endPointsDef +  "<tr class=\"danger\">"+
					"<th>POST</th>"+
					"<th>'/'"+ dbTable + "</th>" + 
					"<th>"+ jsonModel + "</th>" + 
					"</tr>";
	//setting up listner path for the requests
	wirteToConsoleDebug("setting up listener paths");
	//GET request to retreive the records from the table
	app.get('/' + dbTable , function (req, res) {
	
	
	wirteToConsoleDebug("GET request for " + dbTable);
	   var qry = "SELECT * from " + dbTable;
	   wirteToConsoleDebug("query to exec : " + qry);
       con.query(qry, function (err, result) {
		if (err){
			throw err;
		} else {
		wirteToConsoleDebug("result : " + JSON.stringify(result));
		//send the response back to client as a JSON array
		res.send(JSON.stringify(result));
	}
	
  });
   });
   
   //POST request  handling from client to process the incoming JSON body
   var upload = multer(); // for parsing multipart/form-data
   app.use(bodyParser.json()); // for parsing application/json
   app.use(bodyParser.urlencoded({ extended: true })); // for parsing application/x-www-form-urlencoded
   app.post('/' + dbTable , upload.array(), function (req, res, next) {
	   


 // loop through JSON body array from incoming request

	// start looping through the input object array
	for( i in req.body){
		var finResult="";
		var tableFields = "";
		var incomingValues = "";
		wirteToConsoleDebug("Current Object : " + JSON.stringify(req.body[i]));
		//read the message body as a JSON object
		var inputObj = JSON.parse(JSON.stringify(req.body[i]));
		//loop through the table definition
		for( i in tableResult){
			
			//use the table definition as JSON array for collecting the column names 
			// and extract the values from the incoming message
			var tableObjectDef = JSON.parse(JSON.stringify(tableResult[i]));
			wirteToConsoleDebug("Current table column : " + tableObjectDef.Field);
			wirteToConsoleDebug("Current inputObj : " + JSON.stringify(inputObj));
			//tableFields = colllect column names to define the insert statement
			
			var tmpField = "";
			var tmpExtVal = undefined;
			var tmpCurFieldType = "";
			var position = -1;
			tmpCurFieldType= tableObjectDef.Type;
			tmpField = tableObjectDef.Field;
			tmpExtVal = inputObj[tmpField];
			wirteToConsoleDebug("tmp value : " + tmpExtVal);
			wirteToConsoleDebug("tmpField : " + tmpField);
			wirteToConsoleDebug("tmpCurFieldType : " + tmpCurFieldType);
			wirteToConsoleDebug("tmpCurFieldType.indexOf varchar : " + tmpCurFieldType.indexOf("varchar"));
			wirteToConsoleDebug("tmpCurFieldType.indexOf date : " + tmpCurFieldType.indexOf("date"));
			
			
			if(tableFields == ""){
				tableFields = tableObjectDef.Field;
				//check & extract the element value if it's available in the current object

				if(tmpExtVal != undefined){
					//if column name in concern has the values from incoming object
					//use the value to build the values set for insert statement

					if( tmpCurFieldType.indexOf("varchar") >= 0 || tmpCurFieldType.indexOf("date") >= 0){
						incomingValues = incomingValues + "'" + tmpExtVal  + "'" + ",";
					}
					else{
						incomingValues = incomingValues +  tmpExtVal  + ",";
					}
				
				}else{
					//if not leave it .. to fine tune
					incomingValues = incomingValues + "''" + ",";
				}
			}else if(i < tableResult.length-1){
				tableFields = tableFields + "," + tableObjectDef.Field ;
				if(tmpExtVal != undefined){
					//if column name in concern has the values from incoming object
					//use the value to build the values set for insert statement
					if( tmpCurFieldType.indexOf("varchar") >= 0 || tmpCurFieldType.indexOf("date") >= 0 ){
						incomingValues = incomingValues + "'" + tmpExtVal  + "'" + ",";
					}
					else{
						incomingValues = incomingValues +  tmpExtVal  + ",";
					}
				
				}else{
					//if not leave it .. to fine tune
					incomingValues = incomingValues + "''" + ",";
				}
			}else{
				tableFields = tableFields + "," + tableObjectDef.Field;
				if(tmpExtVal != undefined){
					//if column name in concern has the values from incoming object
					//use the value to build the values set for insert statement
					if( tmpCurFieldType.indexOf("varchar") >= 0 || tmpCurFieldType.indexOf("date") >= 0){
						incomingValues = incomingValues + "'" + tmpExtVal  + "'" ;
					}
					else{
						incomingValues = incomingValues +  tmpExtVal  ;
					}
				
				}else{
					//if not leave it .. to fine tune
					incomingValues = incomingValues + "''" ;
				}
			}
			
		}
		
		//build the query to execute
		var insertQry = "INSERT INTO " + dbTable + "(" + tableFields + ")" + " VALUES " + "(" + incomingValues + ")";
		wirteToConsoleDebug("tableFields  : " + tableFields);
		wirteToConsoleDebug("incomingValues  : " + incomingValues);
		wirteToConsoleDebug("Insert query  : " + insertQry);
		//Execute the query
		con.query(insertQry, function (err, result) {
		if (err){
			wirteToConsoleDebug(err);
		} else {
		wirteToConsoleDebug("result : " + JSON.stringify(result));
		finResult = "\n" + result;
		//send the response back to client as a JSON array
		//res.send(JSON.stringify(result));
		}
	});

	}
	//End of for loop
	
	

   
   res.status(201).send(finResult);
   });
   
   	app.get('/', function (req, res) {
		// Synchronous read
		var data = fs.readFileSync('index.html');
		endPointsDef = "<table class=\"table\">" + endPointsDef + "</table>";
		console.log("End points: " + endPointsDef);
		res.send(data.toString().replace("$endPoints",endPointsDef));

	})
}


function wirteToConsoleDebug(logLine)
{
	if(debugEnabled == true){
		console.log(Date.now() + " : " + logLine);
	}
	
}
