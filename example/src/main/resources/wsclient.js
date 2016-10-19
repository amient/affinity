function AffinityWebSocket(location, receiver) {

    var webSocket;
    var wsAddress = "ws://" + location.host + location.pathname + location.search;
    var reconnect = 30

    function internalEnsureOpenSocket() {
        if(webSocket !== undefined && webSocket.readyState !== WebSocket.CLOSED){
           return;
        }

        webSocket = new WebSocket(wsAddress);

        webSocket.onopen = function(event) {
            console.log("WebSocket connection established: " + webSocket.url);
        };

        webSocket.onmessage = receiver;

        webSocket.onclose = function(event){
            if (event.code != 1000) {
                console.log("WebSocket connection interrupted, reconnecting in " + reconnect +" seconds.. " + wsAddress);
                setTimeout(internalEnsureOpenSocket, reconnect * 1000);
            }
        };
    }

    internalEnsureOpenSocket();

    return {
        send: function(text) {
            internalEnsureOpenSocket();
            try {
                webSocket.send(text);
            } catch(err) {
                webSocket.close();
                internalEnsureOpenSocket();
                webSocket.send(text);
            }
        }
    }

}
