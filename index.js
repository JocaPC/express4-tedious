var TYPES = require('tedious').TYPES;

function tediousExpress(req, config){
    
    return function(sqlQueryText) {

        var Connection = require('tedious').Connection;
        var httpRequest = req;
        
        return {
            req: httpRequest,
            connection: new Connection(config),
            sql: sqlQueryText,
            parameters: [],
            param: function(param, value, type){
                this.parameters.push({name: param, type: type, value: value});
                return this;
            },
            exec: function(ostream, successResponse) {
                var request = this.__createRequest(ostream);
                var fnDoneHandler = this.fnOnDone;
                request.on('done', function (rowCount, more, rows) {
                    successResponse && ostream.write(successResponse);
                    fnDoneHandler && fnDoneHandler('done', ostream);
                });
                request.on('doneProc', function (rowCount, more, rows) {
                    successResponse && ostream.write(successResponse);
                    fnDoneHandler && fnDoneHandler('doneProc', ostream);
                });
                this.__ExecuteRequest(request);
            },
            into: function(ostream, defaultOutput) {
                var fnDoneHandler = this.fnOnDone;
                var request = this.__createRequest(ostream);
                var empty = true;
                request.on('row', function (columns) {
                    if(empty) {
                        console.log('Response fetched from SQL Database!');
                        empty = false;
                    }
                    ostream.write(columns[0].value);
                });
                request.on('done', function (rowCount, more, rows) {
                    try{
                        if(empty) {
                            defaultOutput && ostream.write(defaultOutput);
                        }
                    } catch(ex){
                        console.trace(ex);
                    }
                    fnDoneHandler && fnDoneHandler('done', ostream);
                });
                request.on('doneProc', function (rowCount, more, rows) {
                    try{
                        if(empty) {
                            defaultOutput && ostream.write(defaultOutput);
                        }
                    } catch(ex){
                        console.trace(ex);
                    }
                    fnDoneHandler && fnDoneHandler('doneProc', ostream);
                });
                this.__ExecuteRequest(request);
            },
            done: function(fnDone){
                this.fnOnDone = fnDone;
                return this;
            },
            fail: function(fnFail){
                this.fnOnError = fnFail;
                return this;
            },
            __ExecuteRequest: function(request, ostream) {
                var currentConnection = this.connection;
                fnErrorHandler = this.fnOnError;
                currentConnection.on('connect', function (err) {
                    if (err) {
                        console.trace(err);
                        fnErrorHandler && fnErrorHandler(err, ostream);
                    }
                    currentConnection.execSql(request);
                });
            },
            __createRequest: function(ostream){
                var Request = require('tedious').Request;
                var fnErrorHandler = this.fnOnError;
                var fnDoneHandler = this.fnOnDone;
                var request =
                    new Request(this.sql, 
                            function (err, rowCount) {
                                try {
                                    if (err) {
                                        fnErrorHandler && fnErrorHandler(err, ostream);
                                    }
                                }
                                finally{
                                    this.connection && this.connection.close();
                                    fnDoneHandler && fnDoneHandler('Connection closed', ostream);
                                }
                            });

                for(var index in this.parameters) {
                    request.addParameter(
                        this.parameters[index].name,
                        this.parameters[index].type || TYPES.NVarChar,
                        this.parameters[index].value);
                }
                return request;
            },
            fnOnDone: function(message, ostream) {
                try{
                    ostream && ostream.end();
                } catch(ex){
                    console.trace(ex);
                }
            },
            fnOnError: function(error, ostream) {
                try{
                    ostream && ostream.status(500).end();
                } catch (ex) {
                    console.warn("Cannot close response after error: " + ex + "\nOriginal error:"+error);
                }
                console.error(error);
            }
        }
    }
}

module.exports = tediousExpress;