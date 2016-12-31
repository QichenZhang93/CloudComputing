window.onload = function() {

    // Set up CodeMirror
    var codeText = document.getElementById("code");
    var cm = CodeMirror.fromTextArea(codeText, {
        "mode": "python",
        "lineNumbers": true
    })
    cm.setValue("print 'Hello, world'");

    // Set up language selector
    var dropdown = document.getElementById("source-select");
    dropdown.onchange = function(e) {
        cm.setOption("mode", dropdown.value);
    }

    // Set up submit handler
    var codeForm = document.getElementById("source-form");
    codeForm.onsubmit = function(e) {
        e.preventDefault();

        var xhr = new XMLHttpRequest();
        xhr.open("POST", codeForm.action, true);
        xhr.onreadystatechange = function() {
            if (xhr.readyState == 4 && xhr.status == 200) {
                display = document.getElementById("output");
                display.innerHTML = xhr.responseText;
            }
        }
        xhr.send(new FormData(codeForm));
        return false;
    }
}
