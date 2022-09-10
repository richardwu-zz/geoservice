const fs = require('fs');
const os = require('os');
const path = require('path');
const url = require('url');
const cors = require('cors');

const runPyApi = require("./nodeRunPy");
const runPyApiNoSend = require("./nodeRunGeoPy");
const getFileList = require('./nodeUsePy');
const doThumbnail = require('./usePyThumb');
const runPyChkResult = require('./nodeRunPyChk');
const runPyApi2 = require("./nodeRunPy2");


var express = require('express'),
    HttpStatus = require('http-status-codes'),
    morgan = require('morgan'),
    packageConfig = require('./package.json'),
    Busboy = require('busboy');

app = express();

//console.log(os.homedir());
var workDir = os.homedir()+'/richard/ftpServer1';	// here need attension for remote and local
var runJarDir = os.homedir()+'/richard/';
var upldImgFlagRootDir = workDir+'/GeoXpertFiles';
var curDate = ''; // 'in form of 20211124'
var geomesaQryDir = 'geoSearch';
var qryResultDir = workDir+'/'+geomesaQryDir;

function readJsonFile(jsonFile) {
    let bufferData = fs.readFileSync(jsonFile);
    let strData = bufferData.toString();
    let dataArr = JSON.parse(strData);
    return dataArr;
}

function roundDecimal (val, precision) {
	// round with precision
  	return Math.round(Math.round(val * Math.pow(10, (precision || 0) + 1)) / 10) / Math.pow(10, (precision || 0));
}

function folderQryFullInfo(dataArr, theDir) {
	let i = 0;
	let gotItF = false;
	let taskInfoObj = {};
	let retStr = "";
	let findPos = -1;

	while (i < dataArr.length && gotItF == false) {
		taskInfoObj = dataArr[i];
		taskInclFilesStr = taskInfoObj["includeFiles"];
		//console.log(taskInclFilesStr);
		findPos = taskInclFilesStr.indexOf(theDir);
		console.log(findPos);
		if ( findPos >= 0 ) {
			// match
			gotItF = true;
			retStr = taskInfoObj["taskName"];
			parseB = taskInclFilesStr.split('@');
			let j = 0;
			let match = false;
			while( j < parseB.length && match == false) {
				pos2 = parseB[j].indexOf(theDir);
				if ( pos2 >= 0 ) {
					match = true;
					retStr = retStr+'#'+parseB[j];
					console.log(retStr);
				} else {
					j = j+1;
				}
			}
		} else {
			i = i+1;
		}
	}

	if( gotItF == false ) {
		console.log('here in folderQryFullInfo has wrong!');
	}
	return retStr;
}

function fileQryTaskName(dataArr, theImgName) {
	let i = 0;
	let gotItF = false;
	let taskInfoObj = {};
	let retStr = "";
	let findPos = -1;

	while (i < dataArr.length && gotItF == false) {
		taskInfoObj = dataArr[i];
		taskInclFilesStr = taskInfoObj["includeFiles"];
		findPos = taskInclFilesStr.indexOf(theImgName);
		if ( findPos >= 0 ) {
			// match
			gotItF = true;
			retStr = taskInfoObj["taskName"];
			console.log(retStr);
		} else {
			i = i+1;
		}
	}

	if( gotItF == false ) {
		console.log('here in fileQryTaskName has wrong!');
	}
	return retStr;
}


function chkRegularStr(theStr) {
	return /^[A-Za-z0-9]*$/.test(theStr);
}

function writeNote(theStr) {
	// note current time into
	curTimeStr = new Date().toLocaleTimeString().
  					 		replace(/T/, ' ').      // replace T with a space
  							replace(/\..+/, '')     // delete the dot and everything after
  	console.log(curTimeStr);
	// write into todat note file yyyyMMdd.log
	var nowDate = new Date();
    curDate = nowDate.toLocaleDateString('en-GB').split('/').reverse().join(''); // '20211124'
	console.log(curDate);
	curMonth = curDate.substring(0,6);
	txtFilePath = path.join('./dateActionNote', curMonth, curDate+'.log');
	fs.appendFile(txtFilePath, theStr+'|'+curTimeStr+"\n", function (err) {
	  if (err) throw err;
	  console.log('saved!');
	});
}


app.use(cors())

app.use(morgan('combined'))
   .post('/file/uploads?', function (req, res) {
        // take user out from querystring
        //console.log(req);
        var userName = req.query.user;
        console.log(userName);
        var nowDate = new Date();
        curDate = nowDate.toLocaleDateString('en-GB').split('/').reverse().join(''); // '20211124'
        console.log(curDate);
        
        // check user login or not
        console.log(__dirname);
        var userFolder = path.join(__dirname, 'dir/'+curDate+'/'+userName);
        // check if directory exists
		dirExists = fs.existsSync(userFolder); 
		if (dirExists == false) {
		    let parseA = userName.split('_');
		    res.send('plase login first,'+ parseA[0]);
		    res.end();
		} else {
		    //var busboy = new Busboy({headers: req.headers, limits: { files: 1, fileSize: 50000 }});
		    var bb = new Busboy({headers: req.headers});
		    console.log('req headers: '+req.headers);
		    
		    bb.on('file', function (fieldname, file, filename, encoding, mimetype) {
		        console.log('on file upload');
		        file.on('data', function (data) {
		            console.log('File [' + fieldname + '] got ' + data.length + ' bytes');
		        });
		        file.on('end', function () {
		            console.log('File [' + fieldname + '] Finished');
		        });

		        // storage in customer self holder. 
		        var saveTo = path.join(__dirname, 'dir/'+curDate+'/'+userName, path.basename(filename));
		        var outStream = fs.createWriteStream(saveTo);
		        file.pipe(outStream);
		    });
		    
		    bb.on('finish', function () {
		        res.writeHead(HttpStatus.OK, {'Connection': 'close'});
		        res.end("That's all folks!");
		    });
		    
		    return req.pipe(bb);
        }
        
    });

app.get('/', function (req, res) {
    	// http:ipAddr:4000/  to check server alive or not
        res.status(HttpStatus.OK).json({msg: 'OK', service: 'File Server online'})

    }).get('/myip?', function (req, res) {
    	// http:localhost:4000/myip?  to get server ip address
        //res.status(HttpStatus.OK).json({msg: 'OK', service: 'File Server online'})
        runPyApi2.doPyGetIPrun('getIPaddr2.py', './pyCoopUtils', res);
    })
    .get('/api/login?', function (req, res) {

    	const qryStr = req.query;
    	//console.log(qryStr);
    	var nowDate = new Date();
    	curDate = nowDate.toLocaleDateString('en-GB').split('/').reverse().join(''); // '20211124'
	    console.log(curDate);
	    curMonth = curDate.substring(0,6);
	    monthPath = path.join('./dateActionNote', curMonth);
    	if (fs.existsSync(monthPath) == false) {
		    fs.mkdirSync(monthPath);
			}
			userIP = req.socket.remoteAddress;
			console.log('user ip is '+userIP);
    	userName = qryStr.user;
    	passwd = qryStr.passwd;
    	last3Chs = passwd.substring(passwd.length-3);	// here need chg to isalpha() or isnumberic() from and-passwd  
    	retV1 = chkRegularStr(userName);
    	retV2 = chkRegularStr(last3Chs);

    	// write into todat note file yyyyMMdd.log
    	writeNote(userIP+'|'+userName+'_'+last3Chs+'|'+'/api/login?');

    	if( retV1 == true && retV2 == true ) {
    		//console.log(last3Chs);
	        runPyApi.doPyScriptRun('chk2Login.py', './pyCoopUtils', 'users', userName, passwd, res);
	        
	        // generate customer subfolder under date folder.
	        fs.mkdirSync('./dir/'+curDate+'/'+userName+'_'+last3Chs, { recursive: true });
    	} else {
    		res.send('user or passwd is not regular');
    	}
    })
    .get('/api/chkUploads?', function (req, res) {
    	
        const qryStr = req.query;
		console.log(qryStr);
		userName = qryStr.user;
		passwd = qryStr.passwd;
		last3Chs = passwd.substring(passwd.length-3);  
		console.log(last3Chs);
		
		// add for subfolder query
    	subFolder = '';
    	// check query string with folder or not
    	if (( "folder" in qryStr) && qryStr.hasOwnProperty('folder')) {
    		subFolder = qryStr.folder;
    	}
		console.log("folder="+subFolder);
		if( subFolder == '' ) {
			listPath = path.join('GeoXpertFiles', userName+'_'+last3Chs);
		} else {
			listPath = path.join('GeoXpertFiles', userName+'_'+last3Chs, subFolder);
      	}

      	if (fs.existsSync(listPath)) {
      		// list the folder
			resArr = [];
			fs.readdir(listPath, (err, files) => {
			
	  		files.forEach(file => {
	  			// show file for debug
				console.log(file);
				itemObj = {};
				chkObj = path.join(listPath, file);
				const fstats = fs.statSync(chkObj);
	            if( fstats.isDirectory() ) {
	            	// in case zip file extracht subfolder : ex. 20220728_1849
	            	if( file.indexOf('_') > 0 ) {
	            		subFolder = '';
	            		dirObjs = [];
	            		subPath = path.join(listPath, file);
	            		dirObjs = fs.readdirSync( subPath );
	            		if (Array.isArray(dirObjs)) {
		            		dirObjs.forEach( obj => {
		            			// take only sub folder out
		            			parseA = obj.split('.');
		            			//console.log(parseA);
		            			//if( obj.indexOf('.') == -1) {
		            			if( parseA.length == 1) {
									//console.log('extract folder: '+file+'/'+obj);
									itemObj = {};
									itemObj['objName'] = file+'/'+obj;
									itemObj['objType'] = 'folder';
		                			itemObj['fileSize'] = fstats.size.toString();
		                			resArr.push(itemObj);
								} else {
									// chk ext type ? the same file ?
									//console.log('folder/file:'+ obj);
									if(parseA[1].toLowerCase() == 'jpg') {
										//console.log(obj);
										itemObj = {};
										itemObj['objName'] = file+'/'+obj;
										console.log(file+'/'+obj);
										itemObj['objType'] = 'file';
										theFile = path.join(subPath, obj)
										let thestat = fs.statSync(theFile);
										let tmpVal = roundDecimal(thestat.size/1000000, 2);
		                				itemObj['fileSize'] = tmpVal.toString()+'MB';
		                				resArr.push(itemObj);
									}
								}
							})
	            		}
					} else {
						itemObj = {};
	            		itemObj['objName'] = file;
	            		itemObj['objType'] = 'folder';
	                	itemObj['fileSize'] = fstats.size.toString();
	                	resArr.push(itemObj);
	            	}
	                
				} else {
					if( fstats.isFile() ) {
						parseA = file.split('.');
			            fileExt = parseA[parseA.length-1];
			            fileExt = fileExt.toLowerCase();
	            		if(fileExt=='jpg' || fileExt=='zip' ) {
	                		//console.log(file+' is a file');
	                		itemObj = {};
	                  		itemObj['objType'] = 'file';
	                  		itemObj['objName'] = file;
	                  		let tmpVal = roundDecimal(fstats.size/1000000, 2);
	                  		itemObj['fileSize'] = tmpVal.toString()+'MB';
	                  		resArr.push(itemObj);
	                  	}
					}
				}
				
	  		});
	  		
	  		resArrStr = JSON.stringify(resArr);
	  		console.log(resArrStr);
	  		res.send(resArrStr);
			});

      	} else {
      		res.send('empty folder, please check');
      	}

    })
    .get('/api/geosearch?', function (req, res, timeout) {
			// url = http://127.0.0.1/api/geosearch?uname=tippi_ppi&bbox=(geom,x1,y1,x2,y2)
    	// (x1,y1) is BottomRight, (x2,y2) is TopLeft
    	const qryStr = req.query;
    	console.log(qryStr);
    	uname_xxx = qryStr.uname;
    	console.log(uname_xxx);
    	geoQryBbox = qryStr.bbox;
    	console.log(geoQryBbox);

    	// first create the file 'tippi_ppi-bbox.txt' in path:
    	// /home/stdb/richard/ftpServer1/geoSearch/tippi_ppi
    	path1 = workDir+'/'+geomesaQryDir+'/'+uname_xxx;
    	if (fs.existsSync(path1)) {
		    console.log(path1+' is exists');
			} else {
				fs.mkdirSync(path1, { recursive: true });
			}
			// create /home/stdb/richard/ftpServer1/geoSearch/tippi_ppi/tippi_ppi-bbox.txt
    	fs.writeFileSync(path1+'/'+uname_xxx+'-bbox.txt', geoQryBbox);	// for ex. uname_xxx=tippi_ppi
    	
    	// qry count of user's HBase record, spend time info respont to user
    	tblName = 'data';
    	//runPyApi.doPyScriptRun2('tblScannPrefix.py', workDir+'/'+'HBpyTools', tblName, uname_xxx, res);
    	//runPyApiNoSend.doQueryHBaseRun('tblScannPrefix.py', workDir+'/'+'HBpyTools', tblName, uname_xxx);
    	geoSrhType = 'geom-bbox';
    	
    	//runPyApiNoSend.doQueryHBaseRun('qryMiddleware.py', workDir+'/'+'HBpyTools', tblName, uname_xxx+'@'+geoSrhType, res);
    	runPyApiNoSend.doQueryHBaseRun('qryMiddleware.py', workDir+'/'+'HBpyTools', tblName, uname_xxx+'@'+geoSrhType);
    	//runPyApiNoSend.doQueryHBaseRun('qryMiddleware.py', workDir+'/'+'HBpyTools', tblName, uname_xxx+'@'+geoSrhType, res);
    	// -- do mqtt publish, and do this to trigger next action --
    	/*
    	// use mqtt for inner interact
			const mqtt = require('mqtt');
			const host = '127.0.0.1';
			//const portTcp = '1883';
			const portWS = '9001';
			//const connectUrl = 'mqtt://${host}:${portTcp}';
			const connectUrl = 'mqtt://${host}:${portWS}';
			const topicS = '/GeoXpert'+'/'+uname_xxx+'/jobBegin';

	    client = mqtt.connect(connectUrl, {
			  uname_xxx,
			  clean: true,
			  connectTimeout: 4000,
			});
			mqttObj = {}
			mqttObj["sendUser"] = uname_xxx;
			mqttObj["geoType"] = geoSrhType;		//"geom-bbox";
			//mqttObj["qryParams"] = geoQryBbox;
			pubMsgStr = JSON.stringify(mqttObj);
			client.publish(topicS, pubMsgStr, { qos: 0, retain: false }, (error) => {
		    if (error) {
		      console.error(error);
		    }
		    console.log("publish: topic="+topicS+" and msg="+ pubMsgStr);
			});
			*/
			// the remain is python script action and then check the file resultGeoSrh.json exist in python
			runPyChkResult.chkGeoQryResult('dataStoreGeomQry.py', workDir+'/'+'mqttCenter', tblName, uname_xxx+'@'+geoSrhType, res);

    })
    .get('/api/thumbView?', function (req, res) {
    	// for user preview the thumbnail of image file
    	const qryStr = req.query;
    	console.log(qryStr);
    	userName = qryStr.user;
    	passwd = qryStr.passwd;
    	last3Chs = passwd.substring(passwd.length-3);	// here need chg to isalpha() or isnumberic() from and-passwd  
    	console.log(userName+'_'+last3Chs);
    	// fileName
    	filePathName = qryStr.file;
		console.log("file="+filePathName);
    	// do thumbnail of image ~ here fileName is from mqtt pub by jobFinish (it includes HDFS-path)
    	// file='/geoxpert/rich_ich/20220727_2048/DJI_20210908151718_0038.JPG'
		//doThumbnail.doImgThumbnail('py3Imgthumb.py', workDir+'/'+'ImgMetadata', userName+'_'+last3Chs, filePathName, res);
		userIP = req.socket.remoteAddress;
		console.log("user ip ="+userIP);
		doThumbnail.doImgThumbnail('py3Imgthumb.py', workDir+'/'+'ImgMetadata', userName+'_'+last3Chs, filePathName, userIP, res);

    })
    .get(/thumb/, function(req, res) {
    	// for send back thumbView request above
    	urlPathStr = url.parse(req.url).path;
    	sendFilePath = '.'+urlPathStr;
	  	res.sendfile(sendFilePath);
	})
    .get('/api/chkDBinsert?', function (req, res) {
    	const qryStr = req.query;
    	console.log(qryStr);
    	userName = qryStr.user;
    	passwd = qryStr.passwd;
    	tblName = qryStr.table;
    	//last3Chs = passwd.substring(passwd.length-3);	// here need chg to isalpha() or isnumberic() from and-passwd  
    	//console.log(last3Chs);
    	runPyApi.doPyScriptRun('qryTableInsert.py', workDir+'/'+'HBpyTools', tblName, userName, passwd, res);

    }).listen(4000, function () {
      var address = this.address();
      var service = packageConfig.name + ' version: ' + packageConfig.version + ' ';
      console.log('%s Listening on %d', service, address.port);
});
