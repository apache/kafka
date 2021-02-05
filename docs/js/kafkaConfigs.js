$(document).ready(function() {
    $.each([ "brokerconfig", "topicconfig", "producerconfig", "consumerconfig", 
    "connectconfig", "streamsconfig", "adminclientconfig" ], function( index, value ) {
        $("code." + value).each(function (index) {
            const text = $(this).text();
            //console.log(text)
            const re = /(?<!#.*)([a-z0-9.]+)(=.*)?/g;
            var replaced = text.replaceAll(re, function() {
                var frag = `${value}s_${arguments[1]}`;
                //console.log(`Frag ${frag}`);
                if (document.getElementById(frag)) {
                    //console.log(`Arguments: ${arguments.length}`);
                    var rest = arguments[2] === undefined ? "" : arguments[2];
                    return `<a href='#${frag}'>${arguments[1]}</a>${rest}`;
                } else {
                    return arguments[0];
                }
            });
            //console.log(`Replaced ${replaced}`);
            
            $(this).html(replaced)
            // var link = "<a href='#" + value + "s_" + text + "' />"
            // console.log(link)
            // $(this).wrap(link)
        })
    })
})

