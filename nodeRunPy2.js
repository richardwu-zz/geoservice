const HttpStatus = require('http-status-codes');
const PythonShell = require('python-shell').PythonShell;

//					        getIPaddr2.py		       return res handler
function doPyGetIPrun(pyFileName, locate, theRes) {
  //                                path
  //
  // doPyGetIPRun is called from ftpJSserver.js
  // assign options of pythonShell
  //

  var options = {
    mode: 'text',
    pythonPath: '/usr/bin/python3',
    pythonOptions: ['-u'],
    scriptPath: locate,
    args: []
  };

  PythonShell.run(pyFileName, options, function (err, results) {
    if (err) 
      console.log(err);
    // Results is an array consisting of messages collected during execution
    console.log('results: %j', results);
    // respont the result by run of script
    //theRes.send(results[0]);
    theRes.status(HttpStatus.OK).json({queryIP: 'OK', ipAddress: results[0]});

  });

}

module.exports = { doPyGetIPrun };
