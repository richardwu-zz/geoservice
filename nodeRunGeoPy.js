const PythonShell = require('python-shell').PythonShell;

//					           qryMiddleware.py		       data      user
//function doQueryHBaseRun(pyFileName, locate, tblName, uName_xxx, theRes) {
function doQueryHBaseRun(pyFileName, locate, tblName, uName_xxx) {
  //                                   path
  //
  // doPyScriptRun is called from ftpJSserver.js
  // assign options of pythonShell
  //
  var options = {
    mode: 'text',
    pythonPath: '/usr/bin/python3',
    pythonOptions: ['-u'],
    scriptPath: locate,
    args: [tblName, uName_xxx]
  };

  PythonShell.run(pyFileName, options, function (err, results) {
    if (err) 
      console.log(err);
      
    // Results is an array consisting of messages collected during execution
    //console.log('results: %j', results);
    // respont the result by run of script
    //theRes.send(results);
    //return theRes.status(200).json;
  });

}

module.exports = { doQueryHBaseRun };
