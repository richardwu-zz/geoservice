const PythonShell = require('python-shell').PythonShell;
//const fs = require('fs');

//					         dataStoreGeomQry.py	          user@qryType 
function chkGeoQryResult(pyFileName, locate, tblName, paramsStr, theRes) {
  //                                 path     table                res
  // chkGeoQryResult is called from ftpJSserver.js
  // assign options of pythonShell
  // usage: chkGeoQryResult('dataStoreGeomQry.py','mqttCenter', 'rich_ich@geom-bbox', res)
  //

  var options = {
    mode: 'text',
    pythonPath: '/usr/bin/python3',
    pythonOptions: ['-u'],
    scriptPath: locate,
    args: [tblName, paramsStr]
  };

  PythonShell.run(pyFileName, options, function (err, results) {
    if (err) 
      console.log(err);
    
    //
    //mySleep(50000);
    // Results is an array consisting of messages collected during execution
    console.log('results: %j', results);
    theRes.send(results[0]);
       
  });

}

module.exports = { chkGeoQryResult };
