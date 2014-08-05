'use stritct';

var bunyan = require('bunyan');
var Stream = require('stream').Stream;

var logstashStream = exports;


function create(finalStream) {
  var stream = new Stream();

  stream.writable = true;

  stream.write = function(log) {
    var converted = bunyanToLogstash(log);
    str = JSON.stringify(converted, bunyan.safeCycles()) + '\n';

    finalStream.write(str);
  }
  stream.end = function(log) {
    var converted = bunyanToLogstash(log);
    str = JSON.stringify(converted, bunyan.safeCycles()) + '\n';

    finalStream.write(str);

    stream.writable = false;
    process.nextTick(function() { client.close() })
  }

  return stream;
}

function mapLogstashLevel(bunyanLevel) {
  switch (bunyanLevel) {
    case 10 /*bunyan.TRACE*/: return 'TRACE';
    case 20 /*bunyan.DEBUG*/: return 'DEBUG';
    case 30 /*bunyan.INFO*/:  return 'INFO';
    case 40 /*bunyan.WARN*/:  return 'WARN';
    case 50 /*bunyan.ERROR*/: return 'ERROR';
    case 60 /*bunyan.FATAL*/: return 'FATAL';
    default:                  return 'WARN';
  }
}

function flatten(obj, into, prefix, sep) {
  if (into == null) into = {}
  if (prefix == null) prefix = '';
  if (sep == null) sep = '.';
  var key, prop;
  for (key in obj) {
    if (!Object.prototype.hasOwnProperty.call(obj, key)) continue;
    prop = obj[key];
    if (typeof prop === 'object' && !(prop instanceof Date) && !(prop instanceof RegExp))
      flatten(prop, into, prefix + key + sep, sep);
    else
      into[prefix + key] = prop;
  }
  return into;
}

function bunyanToLogstash(log) {
  /*jshint camelcase:false */
  var errFile, key,
      ignoreFields = ['hostname', 'time', 'msg', 'name', 'level', 'v', 'pid'],
      flattenedLog = log,
      logstashMsg = {
        'HOSTNAME':    log.hostname,
        '@timestamp':  log.time,
        message:       log.msg,
        logger_name:   log.name,
        level:         mapLogstashLevel(log.level),
        level_value:   log.level,
        thread_name:   log.pid
      }

  if (log.err && log.err.stack &&
      (errFile = log.err.stack.match(/\n\s+at .+ \(([^:]+)\:([0-9]+)/)) != null) {
    if (errFile[1]) logstashMsg.file = errFile[1];
    if (errFile[2]) logstashMsg.line = errFile[2];
  }

  for (key in log) {
    if (ignoreFields.indexOf(key) < 0 && logstashMsg[key] == null) {
      logstashMsg[key] = flattenedLog[key];
    }
  }

  return logstashMsg;
}

function forBunyan(finalStream) {
  return create(finalStream);
}

logstashStream.create = create;
logstashStream.forBunyan = forBunyan;
logstashStream.bunyanToLogstash = bunyanToLogstash;
logstashStream.mapLogstashLevel = mapLogstashLevel;
logstashStream.flatten = flatten;
