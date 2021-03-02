"use strict";

exports.autocompleteClear = autocompleteId => () => {
    console.log("clearing");
    $("#"+autocompleteId).autoComplete("clear");
};

exports.autocompleteSet = autocompleteId => newValue => () => {
    console.log("setting new value");
    $("#"+autocompleteId).autoComplete("set", newValue);
};

exports.doAutocomplete = autocompleteId => searchCallback => formatCallback => () => {
    $("#"+autocompleteId).autoComplete({
	resolver: "custom",
	formatResult: formatCallback,
	events: {
	    search: function(qry, callback) {
		console.log("searching");
		searchCallback(qry)()
		    .then(result => callback(result))
		    .catch(e => console.log("error searching"));
	    }
	}
    });
};

exports.autocompleteOnChange = autocompleteId => clearCallback => setCallback => () => {
    console.log("subscribing from js to "+autocompleteId);
    $("#"+autocompleteId).on("autocomplete.select", (event, item) => {
	console.log("triggered!");
	if (item === null || item === undefined) {
	    console.log("Item is undefined, clearing via "+clearCallback);
	    clearCallback()();
	}
	else {
	    console.log("Item is defined, setting via "+setCallback);
	    setCallback(item)();
	}
    });
};
