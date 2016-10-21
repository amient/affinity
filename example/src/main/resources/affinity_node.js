window.avro = require('avsc');

window.AvroWebSocket = function (wsAddress, receiver) {

    var webSocket;
    var reconnect = 30;
    var types = new Map();
    var queue = new Array();

    function notifyReceiver(view, type) {
        var bytes = new Uint8Array(view.buffer).subarray(5);
        var record = type.fromBuffer(bytes);
        var _name = type._name.split(".");
        record._name = _name.pop();
        record._namespace = _name.pop();
        while((n=_name.pop()) != null) {
         record._namespace = n + "." + record._namespace;
        }
        console.log(record);
        receiver(record);
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
        types.set(schemaId, type);
        var tmpQueue = new Array();
        while((recordView=queue.pop()) != null){
            if (recordView.getInt32(1, false) == schemaId) {
                notifyReceiver(recordView, type);
            } else {
                tmpQueue.push(recordView);
            }
        }
        queue = tmpQueue;
    }

    function internalEnsureOpenSocket() {
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
                if (!types.has(schemaId)) {
                    queue.push(view);
                    requestSchema(schemaId);
                } else {
                    notifyReceiver(view, types.get(schemaId));
                }
            } else {
                console.error("Magic byte for avro web socket must be either 0 or 123");
            }
        };

        webSocket.onclose = function(event){
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
                webSocket.close();
                internalEnsureOpenSocket();
                webSocket.send(data);
            }
        },
        //TODO sendAvro
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

