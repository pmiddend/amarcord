exports.hide = modalId => () => {
    $('#'+modalId).modal("hide");
}

exports.onHidden = modalId => callback => () => {
    $('#'+modalId).on("hidden.bs.modal", function(e) {
	callback();
    });
}

exports.show = modalId => () => {
    $('#'+modalId).modal("show");
}
