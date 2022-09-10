const PythonShell = require('python-shell').PythonShell;

//					 getUpldList.py       userHolder 	      user ip ret_res
function doImgThumbnail(pyFileName, locate, userID, filePath, userIP, theRes) {
  //                              pyFilepath         imgFilePath
  console.log(pyFileName, locate, userID, filePath);
  // assign options of pythonShell
  var options = {
    mode: 'text',
    pythonPath: '/usr/bin/python3',
    pythonOptions: ['-u'],
    scriptPath: locate,
    args: [userID, filePath, userIP]
  };

  PythonShell.run(pyFileName, options, function (err, results) {
    if (err)
      console.log(err);
    
    // Results is an array consisting of messages collected during execution
    console.log('results: %j', results);
    //theRes.send('http://38.242.216.125:4000/GeoXpertFiles/tippi_ppi/thumb/DJI_20210908153210_0792.JPG');
    // realtime dynamic action
    parseA = filePath.split('/');
    fileName = parseA[parseA.length-1];
    if (results != null) {
      if ( results[0].indexOf(fileName) >= 0 ) {
        // respont the result by run of script
        pos = results[0].indexOf('/GeoXpertFiles')
        if ( pos > 0 ) {
          sendBackStr = 'http://38.242.216.125:4000/GeoXpertFiles/'+userID+'/thumb/'+fileName;
          theRes.send(sendBackStr);
          console.log('thumbFile='+sendBackStr)
        } else {
          theRes.send('something wrong!');
          console.log('something wrong!');
        } 
      }
    } else {
      theRes.send(fileName+' image file lost!');
    }
    
  });

}

module.exports = { doImgThumbnail };
