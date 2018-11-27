window.avro = require('avsc');
window.avro.types = new Map();
window.AvroWebSocket = function (wsAddress, receiver) {

    var webSocket;
    var reconnect = 30;
    var receiveQueue = new Array();
    var sendQueue = new Array();
    var schemas = new Map();

    function notifyReceiver(view, type) {
        var record = type.fromBuffer(Buffer.from(view.buffer.slice(5)));
        var _name = type.name.split(".");
        record._type = type.id
        record._namespace = type.namespace
        while((n=_name.pop()) != null) {
         record._namespace = n + "." + record._namespace;
        }
        receiver(record);
    }

    function sendAvroMessage(type, jsonData) {
        var buf = type.toBuffer(jsonData);
        var requestBuf = new ArrayBuffer(5 + buf.byteLength);
        var wv = new DataView(requestBuf);
        wv.setInt8(0, 0);
        wv.setInt32(1, type.schemaId, false);
        var src = new Uint8Array(buf);
        for (var i = 0; i < src.byteLength; i++) {
            wv.setInt8(i+5, src[i])
        }
        webSocket.send(requestBuf);
    }

    function requestSchema(schemaId) {
        console.log("Requesting Avro Schema ID: " + schemaId);
        var schemaRequest = new ArrayBuffer(5);
        var wv = new DataView(schemaRequest);
        wv.setInt8(0, 123);
        wv.setInt32(1, schemaId, false);
        var request = new Uint8Array(schemaRequest);
        webSocket.send(request);
    }

    function registerSchema(view) {
        var schemaId = view.getInt32(1, false);
        console.log("Received Avro Schema ID: " + schemaId);
        var schema = String.fromCharCode.apply(null, new Int8Array(event.data).subarray(5));
        console.log(schema);
        var type = avro.parse(schema);
        var _name = type.name.split(".");
        type.id = _name.pop();
        type.namespace = _name.pop();
        type.schemaId = schemaId;
        schemas.set(schemaId, type);
        avro.types.set(type.name, type);

        //process pending receive avro data for this schema
        var tmpQueue = new Array();
        while((recordView=receiveQueue.pop()) != null){
            if (recordView.getInt32(1, false) == schemaId) {
                notifyReceiver(recordView, type);
            } else {
                tmpQueue.push(recordView);
            }
        }
        receiveQueue = tmpQueue;
        //process pending send avro data for this schema
        tmpQueue2 = new Array();
        while((sendData=sendQueue.pop()) != null){
            if (sendData.type == type.name) {
                sendAvroMessage(type, sendData.data);
            } else {
                tmpQueue2.push(sendData);
            }
        }
        sendQueue = tmpQueue2;

    }

    function internalEnsureOpenSocket() {
        console.log("internalEnsureOpenSocket for " + webSocket)
        if(webSocket !== undefined && webSocket.readyState !== WebSocket.CLOSED){
           return;
        }

        webSocket = new WebSocket(wsAddress);
        webSocket.binaryType = "arraybuffer";

        webSocket.onopen = function(event) {
            console.log("WebSocket connection established: " + webSocket.url);
        };

        webSocket.onmessage = function (event) {
            if (typeof event.data == "string") {
                //process text message
                receiver(event.data);
                return;
            }
            var view = new DataView(event.data);
            if (view.byteLength == 0) {
                receiver(null);
                return;
            }
            if (view.getInt8(0) == 123) {
                registerSchema(view);
            } else if (view.getInt8(0) == 0) {
                //process object
                var schemaId = view.getInt32(1, false);
                if (!schemas.has(schemaId)) {
                    receiveQueue.push(view);
                    requestSchema(schemaId);
                } else {
                    notifyReceiver(view, schemas.get(schemaId));
                }
            } else {
                console.error("Magic byte for avro web socket must be either 0 or 123");
            }
            //console.log(event)
        };

        webSocket.onclose = function(event){
            //console.log("webSocket.onclose event.code=" + event.code);
            if (event.code != 1000) {
                console.log("WebSocket connection interrupted, reconnecting in " + reconnect +" seconds.. " + wsAddress);
                setTimeout(internalEnsureOpenSocket, reconnect * 1000);
            }
        };
    }

    internalEnsureOpenSocket();

    return {
        sendText: function(data) {
            internalEnsureOpenSocket();
            try {
                webSocket.send(data);
            } catch(err) {
                //webSocket.close();
                internalEnsureOpenSocket();
                webSocket.send(data);
            }
        },
        send: function(avroType, jsonData) {
            var type = avro.types.get(avroType);
            if (type == undefined) {
                console.log("Requesting schema for "+ avroType);
                sendQueue.push( { type: avroType, data: jsonData} );
                webSocket.send(avroType);
            } else {
                sendAvroMessage(type, jsonData);
            }
        },
        sendBinaryUTF8: function (str) {
            var str = unescape(encodeURIComponent(str));
            var charList = str.split('');
            uintArray = [];
            for (var i = 0; i < charList.length; i++) {
                uintArray.push(charList[i].charCodeAt(0));
            }
            webSocket.send(new Uint8Array(uintArray));
        }

    }
}

