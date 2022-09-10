const PythonShell = require('python-shell').PythonShell;

//					        getUpldList.py path  userHolder ret_res
function takeInfoList(pyFileName, locate, userID, theRes) {
  //                                
  console.log(pyFileName, locate, userID);
  // assign options of pythonShell
  var options = {
    mode: 'text',
    pythonPath: '/usr/bin/python3',
    pythonOptions: ['-u'],
    scriptPath: locate,
    args: [userID]
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


module.exports = { takeInfoList };
