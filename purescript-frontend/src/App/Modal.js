exports.hide = modalId => () => {
    $('#'+modalId).modal("hide");
}

exports.show = modalId => () => {
    $('#'+modalId).modal("show");
}
