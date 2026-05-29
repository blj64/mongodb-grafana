var express = require('express');
var bodyParser = require('body-parser');
var _ = require('lodash');
var app = express();
const { MongoClient } = require('mongodb');
var config = require('config');
var Stopwatch = require("statman-stopwatch");
var moment = require('moment')

app.use(bodyParser.json());

// Called by test
app.all('/', async function(req, res, next)
{
  logRequest(req.body, "/")
  setCORSHeaders(res);

  var client = null
  try
  {
    client = createMongoClient(req.body.db)
    await client.connect()
    res.send( { status : "success",
                display_status : "Success",
                message : 'MongoDB Connection test OK' });
    next()
  }
  catch(err)
  {
    res.send({ status : "error",
               display_status : "Error",
               message : 'MongoDB Connection Error: ' + err.message });
    next()
  }
  finally
  {
    if (client != null)
    {
      await client.close()
    }
  }
});

// Called by template functions and to look up variables
app.all('/search', function(req, res, next)
{
  logRequest(req.body, "/search")
  setCORSHeaders(res);

  // Generate an id to track requests
  const requestId = ++requestIdCounter                 
  // Add state for the queries in this request
  var queryStates = []
  requestsPending[requestId] = queryStates
  // Parse query string in target
  queryArgs = parseQuery(req.body.target, {})
  if (queryArgs.err != null)
  {
    queryError(requestId, queryArgs.err, next)
  }
  else
  {
    doTemplateQuery(requestId, queryArgs, req.body.db, res, next);
  }
});

// State for queries in flight. As results come it, acts as a semaphore and sends the results back
var requestIdCounter = 0
// Map of request id -> array of results. Results is
// { query, err, output }
var requestsPending = {}

// Called when a query finishes with an error
function queryError(requestId, err, next)
{
  // We only 1 return error per query so it may have been removed from the list
  if ( requestId in requestsPending )
  {
    // Remove request
    delete requestsPending[requestId]
    // Send back error
    next(err)
  }
}

// Called when query finished
function queryFinished(requestId, queryId, results, res, next)
{
  // We only 1 return error per query so it may have been removed from the list
  if ( requestId in requestsPending )
  {
    var queryStatus = requestsPending[requestId]
    // Mark this as finished
    queryStatus[queryId].pending = false
    queryStatus[queryId].results = results

    // See if we're all done
    var done = true
    for ( var i = 0; i < queryStatus.length; i++)
    {
      if (queryStatus[i].pending == true )
      {
        done = false
        break
      }
    }
  
    // If query done, send back results
    if (done)
    {
      // Concatenate results
      output = []    
      for ( var i = 0; i < queryStatus.length; i++)
      {
        var queryResults = queryStatus[i].results
        var keys = Object.keys(queryResults)
        for (var k = 0; k < keys.length; k++)
        {
          var tg = keys[k]
          output.push(queryResults[tg])
        }
      }
      res.json(output);
      next()
      // Remove request
      delete requestsPending[requestId]
    }
  }
}

// Called to get graph points
app.all('/query', function(req, res, next)
{
    logRequest(req.body, "/query")
    setCORSHeaders(res);

    // Parse query string in target
    substitutions = { "$from" : new Date(req.body.range.from),
                      "$to" : new Date(req.body.range.to),
                      "$dateBucketCount" : getBucketCount(req.body.range.from, req.body.range.to, req.body.intervalMs)
                     }

    // Generate an id to track requests
    const requestId = ++requestIdCounter                 
    // Add state for the queries in this request
    var queryStates = []
    requestsPending[requestId] = queryStates
    var error = false

    for ( var queryId = 0; queryId < req.body.targets.length && !error; queryId++)
    {
      tg = req.body.targets[queryId]
      if (tg.hide || isEmptyQuery(tg.target))
      {
        continue
      }

      queryArgs = parseQuery(tg.target, substitutions)
      if (queryArgs.err != null)
      {
        queryError(requestId, queryArgs.err, next)
        error = true
      }
      else
      {
        queryArgs.type = tg.type
        // Add to the state
        var stateId = queryStates.length
        queryStates.push( { pending : true } )

        // Run the query
        runAggregateQuery( requestId, stateId, req.body, queryArgs, res, next)
      }
    }

    if (!error && queryStates.length == 0)
    {
      delete requestsPending[requestId]
      res.json([])
      next()
    }
  }
);

app.use(function(error, req, res, next) 
{
  // Any request to this server will get here, and will send an HTTP
  // response with the error message
  res.status(500).json({ message: error.message });
});

// Get config from server/default.json
var serverConfig = config.get('server');

app.listen(serverConfig.port);

console.log("Server is listening on port " + serverConfig.port);

function setCORSHeaders(res) 
{
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST");
  res.setHeader("Access-Control-Allow-Headers", "accept, content-type");  
}

function createMongoClient(db)
{
  if (db == null || isEmptyQuery(db.url))
  {
    throw new Error("MongoDB connection error - missing MongoDB URL")
  }

  return new MongoClient(db.url, {
    serverSelectionTimeoutMS: 10000
  })
}

function getMongoDatabase(client, db)
{
  if (db == null || isEmptyQuery(db.db))
  {
    throw new Error("MongoDB connection error - missing MongoDB database name")
  }

  return client.db(db.db)
}

function forIn(obj, processFunc)
{
    var key;
    for (key in obj) 
    {
        var value = obj[key]
        processFunc(obj, key, value)
        if ( value != null && typeof(value) == "object")
        {
            forIn(value, processFunc)
        }
    }
}

function parseQuery(query, substitutions)
{
  doc = {}
  queryErrors = []

  if (isEmptyQuery(query))
  {
    doc.err = new Error('Failed to parse query - Query must start with db.')
    return doc
  }

  query = query.trim() 
  if (query.substring(0,3) != "db.")
  {
    queryErrors.push("Query must start with db.")
  }

  // Query is of the form db.<collection>.aggregate or db.<collection>.find
  // Split on the first ( after db.
  var openBracketIndex = query.indexOf('(', 3)
  if (openBracketIndex == -1)
  {
    queryErrors.push("Can't find opening bracket")
  }
  else
  {
    // Split the first bit - it's the collection name and operation ( must be aggregate )
    var parts = query.substring(3, openBracketIndex).split('.')
    // Collection names can have .s so last part is operation, rest is the collection name
    if (parts.length >= 2)
    {
      doc.operation = parts.pop().trim()
      doc.collection = parts.join('.')       
    }
    else
    {
      queryErrors.push("Invalid collection and operation syntax")
    }
  
    // Args is the rest up to the last bracket
    var closeBracketIndex = query.lastIndexOf(')')
    if (closeBracketIndex == -1)
    {
      queryErrors.push("Can't find last bracket")
    }
    else
    {
      var args = query.substring(openBracketIndex + 1, closeBracketIndex)
      if ( doc.operation == 'aggregate')
      {
        // Wrap args in array syntax so we can check for optional options arg
        args = '[' + args + ']'
        try
        {
          docs = JSON.parse(args)
          // First Arg is pipeline
          doc.pipeline = docs[0]
          if (!Array.isArray(doc.pipeline))
          {
            queryErrors.push("Aggregate pipeline must be an array")
          }
          // If we have 2 top level args, second is agg options
          if ( docs.length == 2 )
          {
            doc.agg_options = docs[1]
          }

          prepareAggregateQuery(doc)
        }
        catch(err)
        {
          queryErrors.push("Invalid aggregate JSON: " + err.message)
        }

        // Replace with substitutions
        if (Array.isArray(doc.pipeline))
        {
          for ( var i = 0; i < doc.pipeline.length; i++)
          {
            var stage = doc.pipeline[i]
            forIn(stage, function (obj, key, value)
                {
                    if ( typeof(value) == "string" )
                    {
                        if ( value in substitutions )
                        {
                            obj[key] = substitutions[value]
                        }
                    }
                })
          }
        }
      }
      else
      {
        queryErrors.push("Unknown operation " + doc.operation + ", only aggregate supported")
      }
    }
  }
  
  if (queryErrors.length > 0 )
  {
    doc.err = new Error('Failed to parse query - ' + queryErrors.join(':'))
  }

  return doc
}

function prepareAggregateQuery(queryArgs)
{
  var flattenPayloadValues = queryArgs.agg_options != null && queryArgs.agg_options.flattenPayloadValues === true

  if (queryArgs.agg_options != null && queryArgs.agg_options.maxDocs != null)
  {
    queryArgs.max_docs = parseMaxDocs(queryArgs.agg_options.maxDocs)
    delete queryArgs.agg_options.maxDocs
  }

  if (!flattenPayloadValues)
  {
    cleanupAggregateOptions(queryArgs)
    return
  }

  delete queryArgs.agg_options.flattenPayloadValues
  if (queryArgs.max_docs == null)
  {
    queryArgs.max_docs = 5000
  }
  if (queryArgs.agg_options.allowDiskUse == null)
  {
    queryArgs.agg_options.allowDiskUse = true
  }
  cleanupAggregateOptions(queryArgs)

  queryArgs.pipeline = getFlattenPayloadValuesPipeline().concat(queryArgs.pipeline)
}

function parseMaxDocs(value)
{
  var maxDocs = Number(value)
  if (!Number.isFinite(maxDocs) || maxDocs <= 0)
  {
    throw new Error("maxDocs aggregate option must be a positive number")
  }

  return Math.floor(maxDocs)
}

function cleanupAggregateOptions(queryArgs)
{
  if (queryArgs.agg_options != null && Object.keys(queryArgs.agg_options).length == 0)
  {
    queryArgs.agg_options = null
  }
}

function getFlattenPayloadValuesPipeline()
{
  return [
    {
      "$match": {
        "payload.values": {
          "$exists": true,
          "$ne": []
        }
      }
    },
    {
      "$unwind": "$payload.values"
    },
    {
      "$addFields": {
        "_grafana_payload_value_entries": {
          "$objectToArray": {
            "$ifNull": [
              "$payload.values.data",
              {}
            ]
          }
        },
        "_grafana_payload_value_time": {
          "$convert": {
            "input": "$payload.values.header.timestamp",
            "to": "date",
            "onError": null,
            "onNull": null
          }
        }
      }
    },
    {
      "$unwind": "$_grafana_payload_value_entries"
    },
    {
      "$replaceRoot": {
        "newRoot": {
          "device_id": "$device_id",
          "source": "$payload.source",
          "datatype": "$payload.datatype",
          "imported_at": "$imported_at",
          "time": "$_grafana_payload_value_time",
          "ts": "$_grafana_payload_value_time",
          "raw_timestamp": "$payload.values.header.timestamp",
          "data_origin": "$payload.values.header.data_origin",
          "program": "$payload.values.header.program",
          "metric": "$_grafana_payload_value_entries.k",
          "value": "$_grafana_payload_value_entries.v.v",
          "value_type": {
            "$type": "$_grafana_payload_value_entries.v.v"
          },
          "raw_value": "$_grafana_payload_value_entries.v"
        }
      }
    }
  ]
}

function isEmptyQuery(query)
{
  return query == null || typeof(query) != "string" || query.trim() == ""
}

// Run an aggregate query. Must return documents of the form
// { value : 0.34334, ts : <epoch time in seconds> }

function runAggregateQuery( requestId, queryId, body, queryArgs, res, next )
{
  runAggregateQueryAsync(requestId, queryId, body, queryArgs, res, next)
}

async function runAggregateQueryAsync( requestId, queryId, body, queryArgs, res, next )
{
  var client = null
  try
  {
    client = createMongoClient(body.db)
    try
    {
      await client.connect()
    }
    catch(err)
    {
      throw new Error("MongoDB connection error: " + err.message)
    }
    const db = getMongoDatabase(client, body.db);

    // Get the documents collection
    const collection = db.collection(queryArgs.collection);
    logQuery(queryArgs.pipeline, queryArgs.agg_options)
    var stopwatch = new Stopwatch(true)

    var docs = null
    try
    {
      docs = await getAggregateDocuments(collection, queryArgs)
    }
    catch(err)
    {
      throw new Error("MongoDB aggregate execution error: " + err.message)
    }

    var results = {}
    try
    {
      if ( queryArgs.type == 'timeserie' )
      {
        results = getTimeseriesResults(docs)
      }
      else
      {
        results = getTableResults(docs)
      }
    }
    catch(err)
    {
      throw new Error("Grafana response format error: " + err.message)
    }

    var elapsedTimeMs = stopwatch.stop()
    logTiming(body, elapsedTimeMs)
    // Mark query as finished - will send back results when all queries finished
    queryFinished(requestId, queryId, results, res, next)
  }
  catch(err)
  {
    queryError(requestId, err, next)
  }
  finally
  {
    if (client != null)
    {
      await client.close()
    }
  }
}

async function getAggregateDocuments(collection, queryArgs)
{
  var cursor = collection.aggregate(queryArgs.pipeline, queryArgs.agg_options)
  if (queryArgs.max_docs == null)
  {
    return await cursor.toArray()
  }

  var docs = []
  while (await cursor.hasNext())
  {
    if (docs.length >= queryArgs.max_docs)
    {
      throw new Error("aggregate returned more than " + queryArgs.max_docs + " documents. Add a $match/$limit stage or increase the maxDocs aggregate option.")
    }
    docs.push(await cursor.next())
  }

  return docs
}

function getTableResults(docs)
{
  var columns = {}
  
  // Build superset of columns
  for ( var i = 0; i < docs.length; i++)
  {
    var doc = docs[i]
    // Go through all properties
    for (var propName in doc )
    {
      // See if we need to add a new column
      if ( !(propName in columns) )
      {
        columns[propName] = 
        {
          text : propName,
          type : getGrafanaColumnType(doc[propName])
        }
      }
    }
  }
  
  // Build return rows
  rows = []
  for ( var i = 0; i < docs.length; i++)
  {
    var doc = docs[i]
    row = []
    // All cols
    for ( var colName in columns )
    {
      var col = columns[colName]
      if ( col.text in doc )
      {
        row.push(doc[col.text])
      }
      else
      {
        row.push(null)
      }
    }
    rows.push(row)
  }
  
  var results = {}
  results["table"] = {
    columns :  Object.values(columns),
    rows : rows,
    type : "table"
  }
  return results
}

function getTimeseriesResults(docs)
{
  var results = {}
  for ( var i = 0; i < docs.length; i++)
  {
    var doc = docs[i]
    validateTimeseriesDoc(doc, i)
    var tg = doc.name
    var dp = null
    if (tg in results)
    {
      dp = results[tg]
    }
    else
    {
      dp = { 'target' : tg, 'datapoints' : [] }
      results[tg] = dp
    }
    
    results[tg].datapoints.push([doc['value'], getTimestampMs(doc['ts'])])
  }
  return results
}

function validateTimeseriesDoc(doc, index)
{
  if (doc.name == null || doc.value == null || doc.ts == null)
  {
    throw new Error("Timeserie query results must include name, value, and ts fields. Missing field in result index " + index)
  }

  if (typeof(doc.value) != "number")
  {
    throw new Error("Timeserie query result value must be a number at result index " + index)
  }

  getTimestampMs(doc.ts)
}

function getTimestampMs(value)
{
  if (value instanceof Date)
  {
    return value.getTime()
  }

  var date = new Date(value)
  if (isNaN(date.getTime()))
  {
    throw new Error("Timeserie query result ts must be a BSON date or ISO date value")
  }

  return date.getTime()
}

function getGrafanaColumnType(value)
{
  if (value instanceof Date)
  {
    return "time"
  }

  if (typeof(value) == "number")
  {
    return "number"
  }

  if (typeof(value) == "boolean")
  {
    return "boolean"
  }

  return "text"
}

// Runs a query to support templates. Must returns documents of the form
// { _id : <id> }
async function doTemplateQuery(requestId, queryArgs, db, res, next)
{
 if ( queryArgs.err == null)
  {
    var client = null
    try
    {
      client = createMongoClient(db)
      try
      {
        await client.connect()
      }
      catch(err)
      {
        throw new Error("MongoDB connection error: " + err.message)
      }
      const database = getMongoDatabase(client, db);
      // Get the documents collection
      const collection = database.collection(queryArgs.collection);
      var result = null
      try
      {
        result = await collection.aggregate(queryArgs.pipeline).toArray()
      }
      catch(err)
      {
        throw new Error("MongoDB aggregate execution error: " + err.message)
      }

      var output = []
      for ( var i = 0; i < result.length; i++)
      {
        var doc = result[i]
        output.push(doc["_id"])
      }
      res.json(output);
      next()
      if ( requestId in requestsPending )
      {
        delete requestsPending[requestId]
      }
    }
    catch(err)
    {
      queryError(requestId, err, next )
    }
    finally
    {
      if (client != null)
      {
        await client.close()
      }
    }
  }
  else
  {
    next(queryArgs.err)
  }
}

function logRequest(body, type)
{
  if (serverConfig.logRequests)
  {
    console.log("REQUEST: " + type + ":\n" + JSON.stringify(body,null,2))
  }
}

function logQuery(query, options)
{
  if (serverConfig.logQueries)
  {
    console.log("Query:")
    console.log(JSON.stringify(query,null,2))
    if ( options != null )
    {
      console.log("Query Options:")
      console.log(JSON.stringify(options,null,2))
    }
  }
}

function logTiming(body, elapsedTimeMs)
{
  if (serverConfig.logTimings)
  {
    var range = new Date(body.range.to) - new Date(body.range.from)
    var diff = moment.duration(range)
    
    console.log("Request: " + intervalCount(diff, body.interval, body.intervalMs) + " - Returned in " + elapsedTimeMs.toFixed(2) + "ms")
  }
}

// Take a range as a moment.duration and a grafana interval like 30s, 1m etc
// And return the number of intervals that represents
function intervalCount(range, intervalString, intervalMs) 
{
  // Convert everything to seconds
  var rangeSeconds = range.asSeconds()
  var intervalsInRange = rangeSeconds / (intervalMs / 1000)

  var output = intervalsInRange.toFixed(0) + ' ' + intervalString + ' intervals'
  return output
}

function getBucketCount(from, to, intervalMs)
{
  var boundaries = []
  var current = new Date(from).getTime()
  var toMs = new Date(to).getTime()
  var count = 0
  while ( current < toMs )
  {
    current += intervalMs
    count++
  }

  return count
}
