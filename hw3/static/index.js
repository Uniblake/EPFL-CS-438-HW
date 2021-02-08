$(document).ready(function () {
    refreshNodebox();
    refreshOriginbox();
    refreshChatbox();
    refreshID();

    // Send text submitted to the chat to the backend as a POST request
    $("#submittext").click(function () {
        var text = $("#text").val();

        var dataToSend = JSON.stringify({ "contents": text });

        const response = fetch("/message", {
            method: 'POST', // *GET, POST, PUT, DELETE, etc.
            mode: 'cors', // no-cors, *cors, same-origin
            cache: 'no-cache', // *default, no-cache, reload, force-cache, only-if-cached
            credentials: 'same-origin', // include, *same-origin, omit
            headers: {
                'Content-Type': 'application/json'
                // 'Content-Type': 'application/x-www-form-urlencoded',
            },
            redirect: 'follow', // manual, *follow, error
            referrerPolicy: 'no-referrer', // no-referrer, *no-referrer-when-downgrade, origin, origin-when-cross-origin, same-origin, strict-origin, strict-origin-when-cross-origin, unsafe-url
            body: dataToSend // body data type must match "Content-Type" header
        });


        //$.post("/message", text);
        //$.post("/message", dataToSend);
    });


    // Send address (ip:port) of a node to add to gossiping
    $("#submitnode").click(function () {
        var addr = $("#node").val();
        $.post("/node", addr);
    });

    // Set my identifier to a given value
    $("#submitid").click(function () {
        var id = $("#identifier").val();
        $.post("/id", id);
    });

    // Download file
    $("#submitdownload").click(function () {
        var dest = $("#download_destination").val();
        var fname = $("#download_filename").val();
        var hash = $("#download_hash").val();
        jsonvalue = JSON.stringify({destination : dest, filename: fname, request: hash})
        $.post("/message", jsonvalue);
    });

    // Initiate search
    $("#submitsearch").click(function () {
        var keywords = $("#search_keywords").val();
        var budget = $("#search_budget").val();
        jsonvalue = JSON.stringify({keywords : keywords, budget: budget})
        $.post("/message", jsonvalue);
    });

    // $("#originbox li").not('.emptyMessage').click(function() {
    //     var clickedId = this.id 
    //     console.log("ready");
    //     var popup = window.open('privatemsg.html', 'privatewindow', 'width=400,height=250');
    //     $(popup).ready(function(){
    //         $("#dest").val(clickedId);
    // });        
    // });


    // $('.private-origin').click(function () {
    //     var origin = $(this).text();
    //     // console.log(origin);
    //     var popup = window.open('privatemsg.html', 'privatewindow', 'width=400,height=400');
    //     $(popup).ready(function(){
    //         console.log("ready");

    //     });
    //     return true;
    // });

    // Send a private message to a peer: change
    $("#submitprivate").click(function () {
        // $.post("/private", text, origin);
        var text = $("#privatetext").val();
        var dest = $("#dest").val()

        var dataToSend = JSON.stringify({ "contents": text, "destination": dest });
        console.log(dataToSend);
        const response = fetch("/message", {
            method: 'POST', // *GET, POST, PUT, DELETE, etc.
            mode: 'cors', // no-cors, *cors, same-origin
            cache: 'no-cache', // *default, no-cache, reload, force-cache, only-if-cached
            credentials: 'same-origin', // include, *same-origin, omit
            headers: {
                'Content-Type': 'application/json'
                // 'Content-Type': 'application/x-www-form-urlencoded',
            },
            redirect: 'follow', // manual, *follow, error
            referrerPolicy: 'no-referrer', // no-referrer, *no-referrer-when-downgrade, origin, origin-when-cross-origin, same-origin, strict-origin, strict-origin-when-cross-origin, unsafe-url
            body: dataToSend // body data type must match "Content-Type" header
        });
        return true;
    });

    // GET request to the backend to obtain all the messages in the chat
    function refreshChatbox() {
        $.getJSON("/message", function (data) {
            var messages = [];
            if (data !== null) {
                for (var i = 0; i < data.length; i++) {
                    messages.push("<li class=\"list-group-item\">\n" +
                        "<p class=\"list-group-item-text\"> <b>" + data[i].Origin +
                        ":</b>  " + data[i].Text + "</p>\n</li>");
                }
            } else {
                messages.push("<li class=\"list-group-item\">\n" +
                    "<p class=\"list-group-item-text\">" + "" + "</p>\n</li>");
            }
            $("#chatbox").html(messages.join("\n"));
        });
        //always scrolling to the bottom of chatbox
        $("#chatbox").scrollTop($("#chatbox")[0].scrollHeight);
    }

    // GET request to the backend to obtain the latest list of gossiping nodes
    function refreshOriginbox() {
        $.getJSON("/origin", function (nodes) {
            console.log("Origin nodes:" + nodes);
            if (nodes !== null) {
                for (var i = 0; i < nodes.length; i++) {
                    nodes[i] = ("<li class=\"list-group-item\">\n" +
                        "<p class=\"list-group-item-text\" id=\"" + nodes[i] + "\">" + nodes[i] + "</p>\n</li>");
                }
                $("#originbox").html(nodes.join("\n"));
            } else {
                var holder = "<li class=\"list-group-item\">\n" +
                    "<p class=\"list-group-item-text\">" + "" + "</p>\n</li>";
                $("#originbox").html(holder);
            }
        });
    }

    // GET request to the backend to obtain the latest list of origin nodes (known routes)
    function refreshNodebox() {
        $.getJSON("/node", function (nodes) {
            console.log("Gossip nodes:" + nodes);
            if (nodes !== null) {
                for (var i = 0; i < nodes.length; i++) {
                    nodes[i] = ("<li class=\"list-group-item\">\n" +
                        "<p class=\"list-group-item-text\">" + nodes[i] + "</p>\n</li>");
                }
                $("#nodebox").html(nodes.join("\n"));
            } else {
                var holder = "<li class=\"list-group-item\">\n" +
                    "<p class=\"list-group-item-text\">" + "" + "</p>\n</li>";
                $("#nodebox").html(holder);
            }
        });
    }

    // GET request to the backend to retrieve my identifier
    function refreshID() {
        $.get("/id", function (id) {
            if (id !== null) {
                $("#identifier").val(id);
            } else {
                $("#identifier").val("none");
            }
        });
    }
    
    // GET request to the backend to retrieve shared files
    function refreshSharedFiles() {
        $.get("/share", function(shared) {
            if (shared !== null && shared !== "") {
                document.getElementById('sharedfiles').innerHTML = shared
            }
        });
    }
    setInterval(refreshSharedFiles, 5000);

    // GET request to the backend to retrieve ordered matched searches
    function refreshMatchedSearches() {
        $.get("/search", function(matched) {
            if (matched !== null && matched !== "") {
                document.getElementById('matched_searches').innerHTML = matched
            } 
        });
    }
    setInterval(refreshMatchedSearches, 5000);

    // Reload Chatbox and Nodebox at given interval
    setInterval(refreshChatbox, 5000);
    setInterval(refreshOriginbox, 5000);
    setInterval(refreshNodebox, 10000);
});