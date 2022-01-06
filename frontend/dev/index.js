let app

function main() {
    app = require("../output/Main/index.js").main();
}

if (module.hot) {
    module.hot.accept(function() {
	app != null && typeof app.kill === "function" && app.kill();
	Array.prototype.forEach.call(
	    document.querySelectorAll("#amarcord-web-app"),
	    function(d) { d.remove() },
	);
	main();
    })
}

main();
