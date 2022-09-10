const PythonShell = require('python-shell').PythonShell;

//					   chk2Login.py		      users		ret res handler
function doPyScriptRun(pyFileName, locate, tblName, userName, userPw, theRes) {
  //                                path
  //
  // doPyScriptRun is called from ftpJSserver.js
  // assign options of pythonShell
  var options = {
    mode: 'text',
    pythonPath: '/usr/bin/python3',
    pythonOptions: ['-u'],
    scriptPath: locate,
    args: [tblName, userName, userPw]
  };

  PythonShell.run(pyFileName, options, function (err, results) {
    if (err) 
      console.log(err);
    // Results is an array consisting of messages collected during execution
    console.log('results: %j', results);
    // respont the result by run of script
    
    theRes.send(results[0]);
  });

}

//					   tblScannPrefix.py	tableName  users   ret res handler
function doPyScriptRun2(pyFileName, locate, tblName, uname_xxx, theRes) {
  //                                path
  //
  // doPyScriptRun is called from ftpJSserver.js
  // assign options of pythonShell
  var options = {
    mode: 'text',
    pythonPath: '/usr/bin/python3',
    pythonOptions: ['-u'],
    scriptPath: locate,
    args: [tblName, uname_xxx]
  };

  PythonShell.run(pyFileName, options, function (err, results) {
    if (err) 
      console.log(err);
    // Results is an array consisting of messages collected during execution
    console.log('results: %j', results);
    // respont the result by run of script
    
    theRes.send(results[0]);
  });

}

module.exports = { doPyScriptRun, doPyScriptRun2 };
