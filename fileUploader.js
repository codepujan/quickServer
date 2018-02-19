var express = require("express"),
    fs = require("fs"),
    rimraf = require("rimraf"),
    mkdirp = require("mkdirp"),
    multiparty = require('multiparty'),
    app = express(),

    // paths/constants
    fileInputName = process.env.FILE_INPUT_NAME || "qqfile",
    publicDir = process.env.PUBLIC_DIR,
    nodeModulesDir = process.env.NODE_MODULES_DIR,
    uploadedFilesPath ='uploaded/',
    chunkDirName = "chunks",
    port = process.env.SERVER_PORT || 8888,
    maxFileSize = process.env.MAX_FILE_SIZE || 0; // in bytes, 0 for unlimited

const Busboy=require('busboy');

const uuidv4 = require('uuid/v4');


let streams=require('memory-streams');


const async = require('async');
const Promise = require('bluebird');
const util=require('util');
let toArray = require('stream-to-array')
const Uuid = require('cassandra-driver').types.Uuid;
const TimeUuid = require('cassandra-driver').types.TimeUuid;


const cassandra = require('cassandra-driver');
const Tuple=require('cassandra-driver').types.Tuple;
const sslOptions = {
  key : fs.readFileSync('/home/ppaudel/.cassandra/cass100.key.pem'),
  cert : fs.readFileSync('/home/ppaudel/.cassandra/cass100.cer.pem'),
  ca : [fs.readFileSync('/home/ppaudel/.cassandra/cass100.cer.pem')]
};


var bodyParser = require('body-parser');
app.use(bodyParser.json()); // support json encoded bodies
app.use(bodyParser.urlencoded({ extended: true })); // support encoded bodies


/**
const busboy=require('connect-busboy')
 app.use(busboy());

app.use(function(req,res,next) {

console.log("Got REQ");

  if (req.busboy) {


req.pipe(req.busboy);
    req.busboy.on('file', function(fieldname, file, filename, encoding, mimetype) {
	file.on('data',function(dd){
	let responseData = {
            success: false
        };

                        res.set("Content-Type", "text/plain");        
		bufferFileUploader(dd,uuidv4(),res,function(resback) {
                responseData.success = true;
	  resback.header('Access-Control-Allow-Origin', '*');
        resback.header('Access-Control-Allow-Methods', 'GET, POST');
        resback.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept')
	console.log("Written File , Check now, DB ");
                resback.send(responseData);
            },
            function(resback) {
                responseData.error = "Problem copying the file!";
		responseData.success=false;
		console.log("Some Prblem unknown with writing file ");
		resback.header('Access-Control-Allow-Origin', '*');
        resback.header('Access-Control-Allow-Methods', 'GET, POST');
        resback.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept')
                resback.send(responseData);
            });
});


	
	});

//next();

          
}else{

console.log("Not BusBoy");

next();

}



});

**/




let client = new cassandra.Client({
  contactPoints : ['10.1.145.100'],
  authProvider : new cassandra.auth.PlainTextAuthProvider('shipenger_test_app', 's4hbM7E2!*B*U*ti#VfvRm'),
  sslOptions : sslOptions,
  promiseFactory : Promise.fromCallback
});

var cors=require('cors');
let bufferData=[];  // For holding the buffer 


let userId='as131123';

//let userId='pujjupaudel';

//ACTUAL USERID : as131123

let userDataBase='test_db';

//ACTUAL USERDATABASE : test_db
// routes
app.use(express.static('public'));
app.use("/node_modules", express.static('node_modules'));
//app.post("/fileUploader", onUpload);
//app.delete("/fileUploader:uuid", onDeleteFile);
//TODO : Need to handle similar case for delete also 

app.use(cors());
app.use(express.json()); // for parsing application/json
app.use(express.urlencoded({extended:true})); //for parsing application/x-www-form-urlencoded

app.post("/imagesbyDataSet",onRequestImagebyDataSet);
app.post("/createDataSet",onCreateDataSet);
app.post("/getDataSet",onRetrieveDataSets);

app.post("/registerUser",onRegisterUser);
app.post("/getUserByName",onGetUserByName);


app.post("/labelColor",onRegisterLabelColor);
app.get("/labelColor",onRetrieveLabelColors);
app.post("/addimagenote",onAddImageNote);



app.post("/getClientId",onGetClientId);
app.post("/downloadSingleImage",onDownloadSingleImage);
app.post("/downloadEntireDataSet",onDownloadEntireDataSet);

app.post("/fileUploader",function(req,res){

 var busboy = new Busboy({ headers: req.headers });
busboy.on('file',function(fieldname,file,filename,encoding,mimetype){

console.log("Field Name ",fieldname);

let responseData = {
            success: false
        };
let imgBuffer=[]
file.on('data',function(data){
imgBuffer.push(data);

});

file.on('end',function(){
let buf=Buffer.concat(imgBuffer);

bufferFileUploader(buf,uuidv4(),res,req.headers.userid,req.headers.datasetid,function(resback) {
                responseData.success = true;
          resback.header('Access-Control-Allow-Origin', '*');
        resback.header('Access-Control-Allow-Methods', 'GET, POST');
        resback.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept')
        console.log("Written File , Check now, DB ");
                resback.send(responseData);
            },
            function(resback) {
                responseData.error = "Problem copying the file!";
                responseData.success=false;
                console.log("Some Prblem unknown with writing file ");
                resback.header('Access-Control-Allow-Origin', '*');
        resback.header('Access-Control-Allow-Methods', 'GET, POST');
        resback.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept')
                resback.send(responseData);
            });

});

});

req.pipe(busboy);

});



/**
app.use(function(req, res, next) {




});

**/

console.log(port);


client.connect(function(err){
   if (err) return console.error(err);
     console.log('Connected to cluster with %d host(s): %j', client.hosts.length, client.hosts.keys());
       console.log('Keyspaces: %j', Object.keys(client.metadata.keyspaces));
//readFileAndWrite();       

console.log("Connected to Cassandra Server ");

app.listen(port);

});



//Query Constant 
const csrf_check_query= 'SELECT count(*) FROM shipenger_test.visitors_by_id where visitor_id = ?';



async function onDownloadSingleImage(req,res){

console.log("Downlaoding Single Image by ImageId ");

let getImage='SELECT * FROM  labelingapp.imagestorage WHERE user_id=? and dataset_name=? and imageid=?';
let value=[req.body.userId,req.body.dataset,req.body.imageId];

let result=await client.execute(getImage,value,{prepare:true});

console.log(result.rows.length);


//send result.rows[0].

let imageString=result.rows[0].imageblob.toString('base64');

//let img=new Buffer(imageString,'base64');

//console.log("Length",img.length);

//res.writeHead(200,{
//'Content-Type':'image/png',
//'Content-Length':img.length
//});

res.send(imageString);


}


async function onDownloadEntireDataSet(req,res){



}
 
async function onGetUserByName(req,res){

console.log("Got Request for getting User details "); 

let query_s = 'SELECT * FROM labelingapp.userRegistration WHERE username=?';
let param_s = [req.body.username];

console.log("Username ",req.body.username);

console.log("Client ID ",req.body.clientId);


let csrf_check=[req.body.clientId];

let result=await client.execute(csrf_check_query,csrf_check,{prepare:true});

console.log(result);

if(!result){
       res.status(403).send("Data not understood");
}

client.execute(query_s,param_s)
  .then(result => {

console.log(result.rows);
if(result.rows.length==0){

//No user OF that name 
//res.send({"success":false,error:"No User",errorCode:110})
console.log("No User Found ");
//let errorMsg="No User Found ";
//res.type('application/json');
//res.status(403).json({error:{message:errorMsg,code:110}});
 res.type('application/json');
  res.json({id:req.body.username,reply:{success:true,error:{},errorCode:-1}});


return;
}
else if(result.rows[0].password==req.body.password)
{
 //res.send({"success":true,error:{},errorCode:-1});
 console.log("Login Succesful ");
  res.type('application/json');
	console.log("Setting Session",req.body.username);
  
res.json({id:req.body.username,reply:{success:true,error:{},errorCode:-1}});
  return;

}else{


console.log("Wrong Password ");

let errorMsg="Wrong Password ";

//res.type('application/json');
//res.status(403).json({error:{message:errorMsg,code:112}});

//Temporary Workaround : 
console.log("Session ID set to ",req.body.username); 

res.type('application/json');
  res.json({id:req.body.username,reply:{success:true,error:{},errorCode:-1}});


return;
}
}
)

  .catch(err => console.log(`Row Selection error: ${err}`));
}




function onRegisterUser(req,res)
{
console.log("Got Request for OnRegisterUser");

const query = 'INSERT INTO labelingapp.userRegistration(username,password,email,institute,accountType,registeredat) VALUES (?,?,?,?,?,?)';

const values = [req.body.username,req.body.password,req.body.email,req.body.institute,req.body.accountType,new Date()];
client.execute(query,values,{ prepare: true })
  .then(result => {
        console.log("The data was inserted succesfully .m Please Check teh database");
res.send({"success":true,"error":{}});
})
    .catch(err => {
        console.log("Error on writing is ",err);
   res.send({"success":false,"error":err});

})
}


async function onCreateDataSet(req,res){
console.log("Request of Creating Data Set ");

console.log(req.body.classes);
console.log(req.body.tags);

let csrf_check=[req.body.clientId];

let result=await client.execute(csrf_check_query,csrf_check,{prepare:true});

console.log(result);

if(!result){
       res.status(403).send("Data not understood");
}


const query = 'INSERT INTO labelingapp.datasetbyuserid(userid,classes,description,name,tags,createdate,total,completed) VALUES (?,?,?,?,?,?,?,?)';
const values = [req.body.username,req.body.classes,req.body.description,req.body.name,req.body.tags,new Date(),req.body.total,0];
client.execute(query,values,{ prepare: true })
  .then(result => {
res.send({"success":true,"error":{}});


})
    .catch(err => {
	console.log("Error on writing is ",err);

})

}


function onAddImageNote(req,res){
const update_note_query='UPDATE labelingapp.imagestorage SET notes=? WHERE user_id=? and dataset_name=? and imageid=?';
const update_values=[req.body.note,req.body.userid,req.body.dataset,req.body.imageid];

client.execute(update_note_query,update_values,{prepare:true})
.then(result=>{

console.log("Note Updated succesfully ");
res.send({"success":true,"error":{}});

})
.catch(err=>{
console.log("Error on writing is ",err);

})

}


async function onRetrieveDataSets(req,res){

console.log("User Id ",req.body.requestId);

let csrf_check=[req.body.clientId];

let result=await client.execute(csrf_check_query,csrf_check,{prepare:true});

console.log(result);

if(!result){
       res.status(403).send("Data not understood");
}




if(req.body.requestId==-1){

console.log("Doing a Public Dataset ");
let query_s = 'SELECT * FROM labelingapp.datasetbyuserid';


client.execute(query_s)
  .then(result => {
res.send(result.rows);
}
)
  .catch(err => console.log(`Row Selection error: ${err}`));
}else{

console.log("Doing a Per User Dataset ");
let query_s = 'SELECT * FROM labelingapp.datasetbyuserid WHERE userid=?';



let param_s = [req.body.requestId];

client.execute(query_s,param_s)
  .then(result => {
res.send(result.rows);
}
)
  .catch(err => console.log(`Row Selection error: ${err}`));



}



}

function onRegisterLabelColor(req,res){

console.log("Request of Creating New Color Label ");

console.log(req.body.label);
console.log(req.body.colorhex);

const query = 'INSERT INTO labelingapp.labelsettingsbydataset(datasetname,label,colorhex) VALUES (?,?,?)';
const values = [userDataBase,req.body.label,req.body.colorhex];
client.execute(query,values,{ prepare: true })
  .then(result => {
console.log("Success");
res.send({"success":true,"error":{}});
})
    .catch(err => {
        console.log("Error on writing is ",err);

})
}

function onRetrieveLabelColors(req,res){

console.log("Retrieving Color Brooo ",req.query.database);
//TODO : on Client Side console.log(req.query.dataSetName);
let query_s = 'SELECT * FROM labelingapp.labelsettingsbydataset WHERE datasetname=?';
let param_s = [req.query.database];

client.execute(query_s,param_s)
  .then(result => {
res.send(result.rows);
}
)
  .catch(err => console.log(`Row Selection error: ${err}`));





}


async function onRequestImagebyDataSet(req,res){

console.log("Requesting From DB here ");
console.log(req.body.dataset);
console.log(req.body.userid);

let csrf_check=[req.body.clientId];

let result=await client.execute(csrf_check_query,csrf_check,{prepare:true});

console.log(result);

if(!result){
       res.status(403).send("Data not understood");
}




let query_s = 'SELECT * FROM labelingapp.imagestorage WHERE user_id=? AND dataset_name=?';


let param_s = [req.body.userid,req.body.dataset];

client.execute(query_s,param_s)
  .then(result => {
console.log("Results Count ",result.rows.length);
parseResponseAndSendBuffer(req,res,result);
//res.send("See the Console Sir ");
}
)
  .catch(err => console.log(`Row Selection error: ${err}`));

}



function parseResponseAndSendBuffer(req,res,cassandraResponse){
let data=[];

console.log("Preparing Response Now ");
for(i=0;i<cassandraResponse.rows.length;i++){
console.log("ID",cassandraResponse.rows[i].imageid);


// Should not happen but also , still just  a check 
if(cassandraResponse.rows[i].imageblob==undefined)
return;

data.push({data:cassandraResponse.rows[i].imageblob.toString('base64'),
imageId:cassandraResponse.rows[i].imageid});
}

//console.log(data);


res.header('Access-Control-Allow-Origin', '*');
        res.header('Access-Control-Allow-Methods', 'GET, POST');
        res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept')
res.send(data);


}


function onGetClientId(req,res){

console.log("Inserting Visitor by Id ");


const insert_visitor_by_id = 'INSERT INTO labelingapp.visitors_by_id (visitor_id, access_time, from_ip, path, plugins, time_zone, user_agent) VALUES (?, ?, ? , ?, ?, ?, ?)';

  const id = Uuid.random();
  const time = new Date();
  const from_ip = req.body.remote_ip;
  const path = req.body.uri;
  const user_agent = req.body.agent;
  let plugins ;
  let time_zone ;
  const values = [id, time, from_ip,path,plugins, time_zone, user_agent];

 res.type('plain');
 
client.execute(insert_visitor_by_id, values, {prepare: true})
  .then(result => {
     console.log(`Row inserted: ${JSON.stringify(result)}`);
     res.send(id.toString());
   })
  .catch(err => {
     console.log(`Row insertion error: ${err}`);
     res.send(err);
   });




}
function onUpload(req, res) {
    
console.log("Got a new Upload Request");



var form = new multiparty.Form();

form.on('part', function(part) {
      	console.log("Got Parsing Data Of Form");
	if (!part.filename) return;

      var size = part.byteCount;
      var name = part.filename;
      var container = 'blobContainerName';
       
});


    form.parse(req, function(err, fields, files) {
        var partIndex = fields.qqpartindex;
	//console.log("Parsed");
	//console.log("Fields",fields);
	//console.log("Files",files);
 	//console.log("Header",files['qqfile'][0].headers);

        // text/plain is required to ensure support for IE9 and older
        res.set("Content-Type", "text/plain");

        if (partIndex == null) {
            onSimpleUpload(fields, files[fileInputName][0], res);
        }
        else {
            onChunkedUpload(fields, files[fileInputName][0], res);
        }
    });

   
}



function onSimpleUpload(fields, file, res) {
    var uuid = fields.qquuid,
        responseData = {
            success: false
        };
    file.name = fields.qqfilename;
    if (isValid(file.size)) {
        moveUploadedFile(file, uuid, function() {
                responseData.success = true;
                  res.header('Access-Control-Allow-Origin', '*');
        res.header('Access-Control-Allow-Methods', 'GET, POST');
        res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept')
                res.send(responseData);
            },
            function() {
                responseData.error = "Problem copying the file!";
                res.send(responseData);
            });
    }
    else {
        failWithTooBigFile(responseData, res);
    }
}



function onChunkedUpload(fields, file, res) {
    var size = parseInt(fields.qqtotalfilesize),
        uuid = fields.qquuid,
        index = fields.qqpartindex,
        totalParts = parseInt(fields.qqtotalparts),
        responseData = {
            success: false
        };

    file.name = fields.qqfilename;

    if (isValid(size)) {
        storeChunk(file, uuid, index, totalParts, function() {
            if (index < totalParts - 1) {
                responseData.success = true;
                res.send(responseData);
            }
            else {
                combineChunks(file, uuid, function() {
                        responseData.success = true;
                        res.send(responseData);
                    },
                    function() {
                        responseData.error = "Problem conbining the chunks!";
                        res.send(responseData);
                    });
            }
        },
        function(reset) {
            responseData.error = "Problem storing the chunk!";
            res.send(responseData);
        });
    }
    else {
        failWithTooBigFile(responseData, res);
    }
}





function failWithTooBigFile(responseData, res) {
    responseData.error = "Too big!";
    responseData.preventRetry = true;
    res.send(responseData);
}

function onDeleteFile(req, res) {
    var uuid = req.params.uuid,
        dirToDelete = uploadedFilesPath + uuid;

    rimraf(dirToDelete, function(error) {
        if (error) {
            console.error("Problem deleting file! " + error);
            res.status(500);
        }

        res.send();
    });
}

function isValid(size) {
    return maxFileSize === 0 || size < maxFileSize;
}

function moveFile(destinationDir, sourceFile, destinationFile, success, failure,onlyfile) {
    mkdirp(destinationDir, function(error) {
        var sourceStream, destStream;


	console.log(onlyfile);

        if (error) {
            console.error("Problem creating directory " + destinationDir + ": " + error);
            failure();
        }
        else {


   sourceStream = fs.createReadStream(sourceFile);
   destStream = fs.createWriteStream(destinationFile);

//       sourceStream.on('data',function(data){
//
//		bufferData+=data;
//	
//		});

	readFileAndWrite(bufferData,destinationFile,destStream,sourceStream,success,failure);
//	sourceStream.on('end',function(){
//		console.log("Completed Streaming Data Here ");
//		readFileAndWrite(bufferData,destinationFile,destStream);
//		});
		//readFileAndWrite(bufferData,destinationFile,destStream,memSourceStream,success,failure);
/**
            sourceStream
                .on("error", function(error) {
                    console.error("Problem copying file: " + error.stack);
                    destStream.end();
                    failure();
                })
                .on("end", function(){
                console.log("File Writing Finished ");
        	fs.writeFile("bufferOutput.png",bufferData);         
	destStream.end();
                    success();
        	//fs.writeFile("bufferOutput.png",bufferData);
   
	     }).on("data",function(data){
              
			 bufferData+=data;
 
		})
                .pipe(destStream);
 **/


 }
    });
}





function moveUploadedFile(file, uuid, success, failure) {
    var destinationDir = uploadedFilesPath + uuid + "/",
        fileDestination = destinationDir + file.name;
    console.log("FIle Destination is ",fileDestination);
    moveFile(destinationDir, file.path, fileDestination, success, failure,file);
}

function storeChunk(file, uuid, index, numChunks, success, failure) {
    var destinationDir = uploadedFilesPath + uuid + "/" + chunkDirName + "/",
        chunkFilename = getChunkFilename(index, numChunks),
        fileDestination = destinationDir + chunkFilename;

    moveFile(destinationDir, file.path, fileDestination, success, failure);
}

function combineChunks(file, uuid, success, failure) {
    var chunksDir = uploadedFilesPath + uuid + "/" + chunkDirName + "/",
        destinationDir = uploadedFilesPath + uuid + "/",
        fileDestination = destinationDir + file.name;


    fs.readdir(chunksDir, function(err, fileNames) {
        var destFileStream;

        if (err) {
            console.error("Problem listing chunks! " + err);
            failure();
        }
        else {
            fileNames.sort();
            destFileStream = fs.createWriteStream(fileDestination, {flags: "a"});

            appendToStream(destFileStream, chunksDir, fileNames, 0, function() {
                rimraf(chunksDir, function(rimrafError) {
                    if (rimrafError) {
                        console.log("Problem deleting chunks dir! " + rimrafError);
                    }
                });
                success();
            },
            failure);
        }
    });
}

function appendToStream(destStream, srcDir, srcFilesnames, index, success, failure) {
    if (index < srcFilesnames.length) {
        fs.createReadStream(srcDir + srcFilesnames[index])
            .on("end", function() {
                appendToStream(destStream, srcDir, srcFilesnames, index + 1, success, failure);
            })
            .on("error", function(error) {
                console.error("Problem appending chunk! " + error);
                destStream.end();
                failure();
            })
            .pipe(destStream, {end: false});
    }
    else {
        destStream.end();
        success();
    }
}

function getChunkFilename(index, count) {
    var digits = new String(count).length,
        zeros = new Array(digits + 1).join("0");

    return (zeros + index).slice(-digits);
}
     





function bufferFileUploader(bufferData,fileName,resObject,userid,userdatabase,success,failure){

console.log("Cassandra Data Insertion Entrry ");
const query = 'INSERT INTO labelingapp.imagestorage (user_id,dataset_name,image_name,imageid,imageblob,segmentedimageblob) VALUES (?,?,?,?,?,?)';
console.log("Image Name is",fileName);
        const values = [userid,userdatabase,fileName+".jpg",fileName,bufferData,bufferData];
client.execute(query,values,{ prepare: true })
  .then(result => {
console.log("File writing Succes id",fileName);

success(resObject);
})
    .catch(err => {
console.log("Error",err);
failure(resObject);
})
  //})

}


function readFileAndWrite(bufferData,fileName,destStream,sourceStream,success,failure){
 
console.log("Cassandra Data Insertion Entrry ");
console.log("ImageName",fileName);
const query = 'INSERT INTO labelingapp.imagestorage (user_id,dataset_name,image_name,imageblob) VALUES (?,?,?,?)';
let buffer;
toArray(sourceStream)
  .then(function (parts) {
    const buffers = parts
      .map(part => util.isBuffer(part) ? part : Buffer.from(part));
        buffer=Buffer.concat(buffers);
	const values = [userId,userDataBase,fileName,buffer];
        console.log("Buffer is ",buffer);
client.execute(query,values,{ prepare: true })
  .then(result => {

destStream.end();
success();
})
    .catch(err => {
console.log("Error",err);
destStream.end();
failure();
})
  })

}


